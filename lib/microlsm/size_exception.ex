defmodule Microlsm.SizeException do
  defexception [:message, :term, :size, :max_size]

  def check!(size, max_size, term \\ nil)

  def check!(size, max_size, term) when size >= max_size do
    raise __MODULE__,
      size: size,
      maximum_size: max_size,
      term: term,
      message: "Violation of the size limit in encoded format"
  end

  def check!(_size, _max_size, _term) do
    :ok
  end
end
