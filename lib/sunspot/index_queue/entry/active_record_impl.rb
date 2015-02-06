require 'active_record'
require 'benchmark'

module Sunspot
  class IndexQueue
    module Entry
      # Implementation of an indexing queue backed by ActiveRecord.
      #
      # To create the table, you should have a migration containing the following:
      #
      #   self.up
      #     Sunspot::IndexQueue::Entry::ActiveRecordImpl.create_table
      #   end
      #
      #   self.down
      #     drop_table Sunspot::IndexQueue::Entry::ActiveRecordImpl.table_name
      #   end
      #
      # The default set up is to use an integer for the +record_id+ column type since it
      # is the most efficient and works with most data models. If you need to use a string
      # as the primary key, you can add additional statements to the migration to do so.
      class ActiveRecordImpl < ActiveRecord::Base
        include Entry
        
        self.table_name = "sunspot_index_queue_entries"

        before_save :set_queue_position_column

        class << self
          def logger=(logger)
            @logger = logger
          end

          def logger
            @logger
          end
          # Implementation of the total_count method.
          def total_count(queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            count(:conditions => conditions)
          end
          
          # Implementation of the ready_count method.
          def ready_count(queue)
            conditions = ["#{connection.quote_column_name('run_at')} <= ?", Time.now.utc]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            count(:conditions => conditions)
          end

          # Implementation of the error_count method.
          def error_count(queue)
            conditions = ["#{connection.quote_column_name('error')} IS NOT NULL"]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            count(:conditions => conditions)
          end

          # Implementation of the errors method.
          def errors(queue, limit, offset)
            conditions = ["#{connection.quote_column_name('error')} IS NOT NULL"]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            all(:conditions => conditions, :limit => limit, :offset => offset, :order => :id)
          end

          # Implementation of the reset! method.
          def reset! (queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            update_all({:run_at => Time.now.utc, :attempts => 0, :error => nil, :lock => nil}, conditions)
          end
         
          # Implementation of the next_batch! method. 
          def next_batch!(queue)
            b_gen_conditions = Benchmark.measure do
              conditions = ["#{connection.quote_column_name('run_at')} <= ?", Time.now.utc]
              unless queue.class_names.empty?
                conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
                conditions << queue.class_names
              end
            end
            profile 'Generating conditions', b_gen_conditions

            b_select_entries = Benchmark.measure do
              batch_entries = all(:select => "id", :conditions => conditions, :limit => queue.batch_size, :order => 'queue_position')
              queue_entry_ids = batch_entries.collect{|entry| entry.id}
            end
            profile 'Selecting entries', b_select_entries

            return [] if queue_entry_ids.empty?
            lock = rand(0x7FFFFFFF)
            b_update_entries = Benchmark.measure do
              update_all({:run_at => queue.retry_interval.from_now.utc, :lock => lock, :error => nil}, :id => queue_entry_ids)
            end
            profile 'Queuing entries', b_update_entries

            b_retrieve_entries = Benchmark.measure do
              entries = all(:conditions => {:id => queue_entry_ids, :lock => lock})
              entries.first
            end
            profile 'Retrieving entries', b_retrieve_entries
            entries
          end
          # Alternative implementation (indexes would have to be changed).
          # It performs two queries instead of three and may prevent workers
          # competing for elements to process while trying to lock rows that
          # have already been locked by another worker. Even though the update
          # query is complex and may undermine database performance.
          #
          # def next_batch!(queue)
          #   conditions = ["#{connection.quote_column_name('run_at')} <= ?", Time.now.utc]
          #   unless queue.class_names.empty?
          #     conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
          #     conditions << queue.class_names
          #   end
          #   batch_entries = where(:conditions => conditions).limit(queue.batch_size).order('queue_postion')
          #   lock = rand(0x7FFFFFFF)
          #   batch_entries.select(:id).update_all run_at: queue.retry_interval.from_now.utc, lock: lock, error: nil
          #   batch_entries.where lock: lock
          # end


          # Implementation of the add method.
          def add(klass, id, delete, priority)
            queue_entry_key = {:record_id => id, :record_class_name => klass.name, :lock => nil}
            queue_entry = first(:conditions => queue_entry_key) || new(queue_entry_key.merge(:priority => priority))
            queue_entry.is_delete = delete
            queue_entry.priority = priority if priority > queue_entry.priority
            queue_entry.run_at = Time.now.utc
            queue_entry.save!
          end
          
          # Implementation of the delete_entries method.
          def delete_entries(entries)
            delete_all(:id => entries)
          end
          
          # Create the table used to store the queue in the database.
          def create_table
            connection.create_table table_name do |t|
              t.string :record_class_name, :null => false
              t.integer :record_id, :null => false
              t.boolean :is_delete, :null => false, :default => false
              t.datetime :run_at, :null => false
              t.integer :priority, :null => false, :default => 0
              t.integer :lock, :null => true
              t.string :error, :null => true, :limit => 4000
              t.integer :attempts, :null => false, :default => 0
            end

            connection.add_index table_name, :record_id
            connection.add_index table_name, [:run_at, :record_class_name, :priority], :name => "#{table_name}_run_at"
          end

          def profile(message, benchmark)
            logger.info "====== #{message} ======"
            logger.info benchmark.to_s
            puts "====== #{message} ======"
            puts benchmark.to_s
          end
        end

        # Implementation of the set_error! method.
        def set_error!(error, retry_interval = nil)
          self.attempts += 1
          self.run_at = (retry_interval * attempts).from_now.utc if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          self.priority = -1
          self.lock = nil
          begin
            save!
          rescue => e
            if logger
              logger.warn(error)
              logger.warn(e)
            end
          end
        end

        # Implementation of the reset! method.
        def reset!
          begin
            update_attributes!(:attempts => 0, :error => nil, :lock => nil, :run_at => Time.now.utc)
          rescue => e
            logger.warn(e)
          end
        end

        # From MYSQL doc:
        # ---------------------------------------------------------------------
        # In some cases, MySQL cannot use indexes to resolve the ORDER BY, 
        # although it still uses indexes to find the rows that match the 
        # WHERE clause. These cases include the following:
        #
        # - You use ORDER BY on different keys:
        #
        #   SELECT * FROM t1 ORDER BY key1, key2;
        # ---------------------------------------------------------------------
        #
        # The described condition applies to our query to sort the elements to
        # be processed since the conditions are to sort by priority DESC and
        # run_at ASC. To boost database performance a new column has been 
        # added containing both informations encoded in such a fashion that a 
        # sort applied on a set of entries returns the entries in the expected 
        # order. The column is called queue_position and the format of its
        # values is:
        # 
        # "2.0000000000-0001423053863"
        #
        # Which represents the inverse value of the priority - the unix time of
        # the run_at with a padding of zeros. The ASC ordering on those strings
        # is equivalent to the composed order on priority DESC, run_at ASC.
        def set_queue_position_column
          raise "WTF!! Negative priority?" if priority < 0
          raise "Priority should not be bigger than 2^10" if priority > 20000000000
          
          inverse_priority = priority == 0 ? 2 : 1/priority.to_f
          fixed_width_time = run_at.to_i.to_s[0...13].rjust(13, '0')

          self.queue_position = format("%0.10f", inverse_priority) + '-' + fixed_width_time
        end
      end
    end
  end
end
