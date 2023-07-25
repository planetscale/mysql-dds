class Sketch
  def initialize(version: 1, gamma: (1 + 0.01) / (1 - 0.01), vals:)
    @version = version
    @sum = vals.reduce(0, &:+)
    @count = vals.count
    @gamma = gamma
    @buckets = Hash.new(0)

    vals.each do |val|
      @buckets[Math.log(val, @gamma).ceil] += 1
    end
  end

  def raw
    out = [ @version, @gamma, @sum,].pack("Cee")

    out += self.class.varint(@count)

    prev_key = 0
    @buckets.sort_by(&:first).each do |key, count|
      out += self.class.varint(key - prev_key)
      out += self.class.varint(count)
      prev_key = key
    end
    out
  end

  def hex
    raw.unpack("H*").first
  end

  def self.varint(int)
    raise "Refusing to encode varint for negative number #{int}" if int < 0

    out = ""
    loop do
      byte = int & 0b0111_1111
      int >>= 7
      if int == 0
        out << byte.chr
        break
      else
        out << (byte | 0b1000_0000).chr
      end
    end
    out
  end

  # return a new sketch equal to self merged with other
  def +(other)
    fail unless @version == other.version
    fail unless @gamma == other.gamma
    ret = Sketch.new(vals: [])
    ret.sum = @sum + other.sum
    ret.count = @count + other.count
    [self, other].each do |sk|
      sk.buckets.each do |k, v|
        ret.buckets[k] += v
      end
    end
    ret
  end

protected
  attr_accessor :version, :sum, :count, :gamma, :buckets
end
