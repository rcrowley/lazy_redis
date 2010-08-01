require 'redis'
require 'set'

class LazyRedis

  def initialize(redis=nil)
    @redis = redis || Redis.new
    @local = {}
  end

  attr_reader :redis

  def sync
    @local.each { |key, value| value.sync }
  end

  def [](key)
    return @local[key] if @local.has_key?(key)
    @local[key] = self.class.const_get(
      @redis.type(key).to_s.capitalize).new(@redis, key)
  end

  def []=(key, value)
    @local[key] = case value
    when None, String, List, Set, Zset, Hash
      value
    when ::NilClass
      None.new(@redis, key)
    when ::String
      String.new(@redis, key, value)
    when ::Array
      List.new(@redis, key, value)
    when ::Set
      Set.new(@redis, key, value)
    when ::Hash
      # TODO
    else
      raise # TODO Raise an appropriate exception.
    end
  end

  def method_missing(symbol, *args)
    # TODO Implement the standard Redis API here.
  end

  class None

    def initialize(redis, key, value=nil)
      @redis, @key, @value = redis, key, nil
    end

    def sync
      @redis.del @key
    end

    attr_reader :value

    # TODO Morph into the appropriate type as implied by method calls.

  end

  class String

    def initialize(redis, key, value=nil)
      @redis, @key, @value = redis, key, value
    end

    def sync
      @redis.set @key, @value
    end

    attr_accessor :value

    def get
      @value ||= @redis.get(@key)
    end

    def set(value)
      @value = value
    end

    # TODO

  end

  class List

    def initialize(redis, key, value=nil)
      @redis, @key = redis, key
      @value = value.respond_to?(:to_ary) ? value.to_ary : nil
      @left, @removed, @right = [], [], []
    end

    def sync
      if @value.respond_to?(:to_ary)
        @redis.del @key
        @value.each { |value| @redis.rpush value }
      end
      @left.reverse.each { |value| @redis.lpush @key, value }
      @right.each { |value| @redis.rpush @key, value }
      @removed.each { |value, count| @redis.lrem @key, count, value }
    end

    attr_accessor :value

    def rpush(value)
      @right.push value
    end

    def lpush(value)
      @left.unshift value
    end

    def llen
      @value.respond_to?(:to_ary) ? @value.length : 0 +
        @left.length - @removed.length + @right.length +
        @value.nil? ? @redis.llen(@key) : 0
    end

    def lrange(s, e)
      # TODO
    end

    def ltrim(s, e)
      # TODO
    end

    def lindex(i)
      # TODO
    end

    def lset(i, value)
      # TODO
    end

    # We break with the Redis API here because we can't know for sure
    # how many values are actually removed.  Further inconsistencies:
    #   An lrem before an lpush or rpush will actually happen after
    #   the lpush or rpush, making it unsafe for use with partially
    #   remote lists.
    #   FIXME The behavior with negative count is the same as if
    #   count is zero.
    def lrem(value, count=1)
      @left.each do |value|
        break unless @left.delete(value)
        return if 1 == count
        count -= 1
      end
      if @value.respond_to?(:to_ary)
        @value.each do |value|
          break unless @left.delete(value)
          return if 1 == count
          count -= 1
        end
      end
      @right.each do |value|
        break unless @left.delete(value)
        return if 1 == count
        count -= 1
      end
      @removed << [value, count]
    end

    def lpop
      if 0 < @left.length
        @left.shift
      elsif @value.respond_to?(:to_ary) && 0 < @value.length
        @value.shift
      elsif value = @redis.lpop(@key)
        value
      elsif 0 < @right.length
        @right.shift
      end
    end

    def rpop
      if 0 < @right.length
        @right.pop
      elsif @value.respond_to?(:to_ary) && 0 < @value.length
        @value.pop
      elsif value = @redis.rpop(@key)
        value
      elsif 0 < @left.length
        @left.pop
      end
    end

    def blpop
      if 0 < @left.length
        @left.shift
      elsif @value.respond_to?(:to_ary) && 0 < @value.length
        @value.shift
      else
        @redis.blpop @key
      end
    end

    def brpop
      if 0 < @right.length
        @right.pop
      elsif @value.respond_to?(:to_ary) && 0 < @value.length
        @value.pop
      else
        @redis.brpop @key
      end
    end

    # TODO rpoplpush

  end

  class Set

    def initialize(redis, key, value=nil)
      @redis, @key = redis, key
      @value = case set
      when ::NilClass
        nil
      when ::Array
        Set.new(set)
      when ::Set
        set
      else
        raise # TODO Raise an appropriate exception.
      end
    end

    def sync
      # TODO
    end

    attr_accessor :value

    # TODO

  end

  class Zset

    def initialize(redis, key, value=nil)
      @redis, @key, @value = redis, key, value
    end

    def sync
      # TODO
    end

    attr_accessor :value

    # TODO

  end

  class Hash

    def initialize(redis, key, value=nil)
      @redis, @key, @value = redis, key, value
    end

    def sync
      # TODO
    end

    attr_accessor :value

    # TODO

  end

end

lazy = LazyRedis.new

=begin
foo = lazy["foo"]
puts foo.inspect
lazy["foo"] = "foo foo"
foo = lazy["foo"]
puts foo.inspect
puts foo.get
foo.set "foo bar"
puts foo.get
=end

lazy["foo"] = nil
lazy.sync
puts lazy.redis.type("foo").inspect

lazy["foo"] = LazyRedis::List.new(lazy.redis, "foo") # TODO Better interface.
foo = lazy["foo"]
foo.lrem "bar"
foo.rpush "bar"
lazy.sync
puts lazy.redis.lrange("foo", 0, 10).inspect
foo.rpush "baz"
foo.lpush "foo"
puts "baz: #{foo.rpop}"
foo.rpush "bar"
foo.rpush "baz"
lazy.sync
puts lazy.redis.type("foo").inspect
puts lazy.redis.lrange("foo", 0, 10).inspect

require 'pp'
#pp lazy
