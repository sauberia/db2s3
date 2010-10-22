begin
  require 'active_support' # The new one
rescue LoadError
  require 'activesupport' # The old one
end
require 's3'
require 'tempfile'

class DB2S3
  class Config
  end

  def initialize
  end

  def full_backup
    file_name = dump_file_name(Time.now)
    store.store(file_name, open(dump_db.path))
    store.store(most_recent_dump_file_name, file_name)
  end

  def restore
    dump_file_name = store.fetch(most_recent_dump_file_name).read
    file = store.fetch(dump_file_name)
    run "gunzip -c #{file.path} | mysql #{mysql_options}"
  end

  # TODO: This method really needs specs
  def clean
    files = file_objects_from_paths(store.list("#{dump_file_name_prefix}-"))
    determine_what_to_keep files
    delete_surplus_backups files
  end
  
  def file_objects_from_paths paths
    paths.collect do |path| {
        :path => path,
        :date => Time.parse(path.split('-').last.split('.').first),
        :keep => false
      }
    end
  end
  
  def determine_what_to_keep files
    # Keep all backups from the past day
    files.select {|x| x[:date] >= 1.day.ago }.map! do |backup_for_day|
      backup_for_day[:keep] = true
    end
    # Keep one backup per day from the last week
    files.select {|x| x[:date] >= 1.week.ago }.group_by {|x| x[:date].strftime("%u") }.values.map! do |backups_for_last_week|
      backups_for_last_week.sort_by{|x| x[:path] }.first[:keep] = true
    end
    # Keep one backup per week from the last 28 days
    files.select {|x| x[:date] >= 28.days.ago }.group_by {|x| x[:date].strftime("%Y%W") }.values.map! do |backups_for_last_28_days|
      backups_for_last_28_days.sort_by{|x| x[:path] }.first[:keep] = true
    end
    # Keep one backup per month since forever
    files.group_by {|x| x[:date].strftime("%Y%m") }.values.map! do |backups_for_month|
      backups_for_month.sort_by{|x| x[:path] }.first[:keep] = true
    end
  end
  
  def delete_surplus_backups files
    files.each do |file|
      store.delete(file[:path]) unless file[:keep]
    end
  end

  def statistics
      # From http://mysqlpreacher.com/wordpress/tag/table-size/
    results = ActiveRecord::Base.connection.execute(<<-EOS)
    SELECT
      engine,
      ROUND(data_length/1024/1024,2) total_size_mb,
      ROUND(index_length/1024/1024,2) total_index_size_mb,
      table_rows,
      table_name article_attachment
      FROM information_schema.tables
      WHERE table_schema = '#{db_credentials[:database]}'
      ORDER BY total_size_mb + total_index_size_mb desc;
    EOS
    rows = []
    results.each {|x| rows << x.to_a }
    rows
  end

  private

  def dump_db
    dump_file = Tempfile.new('dump')
    cmd = "mysqldump --quick --single-transaction --create-options #{mysql_options}"
    cmd += " | gzip > #{dump_file.path}"
    run(cmd)

    dump_file
  end

  def mysql_options
    cmd = ''
    cmd += " -u #{db_credentials[:user]} "     unless db_credentials[:user].nil?
    cmd += " -p'#{db_credentials[:password]}'" unless db_credentials[:password].nil?
    cmd += " -h '#{db_credentials[:host]}'"    unless db_credentials[:host].nil?
    cmd += " #{db_credentials[:database]}"
  end

  def store
    @store ||= S3Store.new
  end
  
  def dump_file_name_prefix
    "dump-#{db_credentials[:database]}-"
  end
  
  def dump_file_name time
    "#{dump_file_name_prefix}-#{time.utc.strftime("%Y%m%d%H%M%S")}.sql.gz"
  end

  def most_recent_dump_file_name
    "most-recent-#{dump_file_name_prefix}.txt"
  end

  def run(command)
    result = system(command)
    raise("error, process exited with status #{$?.exitstatus}") unless result
  end

  def db_credentials
    ActiveRecord::Base.connection.instance_eval { @config } # Dodgy!
  end

  class S3Store
    def initialize
      @connected = false
    end

    def ensure_connected
      return if @connected
      s3_service = S3::Service.new(DB2S3::Config::S3.slice(:access_key_id, :secret_access_key).merge(:use_ssl => true))
      @bucket = s3_service.buckets.build(DB2S3::Config::S3[:bucket])
      @connected = true
    end

    def store(file_name, file)
      ensure_connected
      object = bucket.objects.build(file_name)
      object.content = file.class == String ? file : (file.rewind; file.read)
      object.save
    end

    def fetch(file_name)
      ensure_connected
      file = Tempfile.new('dump')
      file.binmode if file.respond_to?(:binmode)
      file.write(bucket.objects.find(file_name).content)
      file.rewind
      file
    end

    def list(prefix)
      ensure_connected
      bucket.objects.find_all(:prefix => prefix).collect {|x| x.key }
    end

    def delete(file_name)
      if object = bucket.objects.find(file_name)
        object.delete
      end
    end

    private

    def bucket
      @bucket
    end
  end

end
