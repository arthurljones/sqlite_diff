class Logger
  def initialize
    @nesting_level = 0
  end

  def nest(message)
    prefix = " " * 2 * @nesting_level
    puts("#{prefix}#{message}")

    if block_given?
      @nesting_level += 1
      result = yield
      @nesting_level -= 1
      result
    end
  end
end
