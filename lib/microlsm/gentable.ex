defmodule Microlsm.Gentable do
  @moduledoc false
  alias Microlsm.BloomFilter
  alias Microlsm.Disktable
  alias Microlsm.DescriptorPool

  import Record, only: [defrecord: 2]
  import Disktable, only: [is_inbound: 1]
  import :erlang, only: [element: 2]

  @type generation_number :: non_neg_integer()

  @typedoc "Tuple of `t:disktable/0`"
  @type generation :: :erlang.tuple()

  @opaque table :: :ets.tab()

  defrecord :disktable,
    bloom_filter: nil,
    descriptor_pool_state: nil,
    filename: nil,
    generation: nil,
    id: nil,
    index: nil

  @type disktable :: {
    :disktable,
    bloom_filter :: BloomFilter.t(),
    descriptor_pool_state :: DescriptorPool.t(),
    filename :: Path.t(),
    generation :: generation_number(),
    id :: term(),
    index :: Disktable.index()
  }

  @spec new() :: table()
  def new do
    :ets.new(:microlsm_generations, [:public, :ordered_set, read_concurrency: true, write_concurrency: false])
  end

  @spec insert(table(), [{non_neg_integer(), [disktable()]}]) :: :ok
  def insert(table, generations) do
    generations =
      Enum.map(generations, fn
        {generation, entries} when is_list(entries) ->
          entries = Enum.sort_by(entries, fn disktable(index: index) -> index end)
          {generation, List.to_tuple(entries)}

        {generation, entries} when is_tuple(entries) ->
          {generation, entries}
      end)

    :ets.insert(table, generations)
    :ok
  end

  @spec clear(table()) :: :ok
  def clear(table) do
    :ets.delete_all_objects(table)
    :ok
  end

  @spec remove(table()) :: :ok
  def remove(table) do
    for {_, disktables} <- to_list(table), dt <- Tuple.to_list(disktables) do
      disktable(filename: filename) = dt
      {:ok, fd} = :prim_file.open(filename, [:read, :write])
      Disktable.truncate(fd)
      :prim_file.close(fd)
      File.rm!(filename)
    end

    clear(table)

    :ok
  end

  @spec to_list(table()) :: [{generation_number(), {disktable()}}]
  def to_list(table) do
    :ets.match_object(table, :_)
  end

  @spec search_disktable_in_generation(generation(), term()) :: disktable() | :not_found
  def search_disktable_in_generation({disktable}, _key) do
    disktable
  end

  def search_disktable_in_generation(disktables, key) do
    do_search_disktable_in_generation(disktables, key, 1, tuple_size(disktables))
  end

  # Binary searching the disktable in the tuple
  defp do_search_disktable_in_generation(disktables, key, l, r) do
    case div(l + r, 2) do
      ^l ->
        disktable = disktable(index: index) = element(l, disktables)

        case Disktable.check_bounds(index, key) do
          result when is_inbound(result) ->
            disktable

          :higher ->
            disktable = disktable(index: index) = element(r, disktables)
            case Disktable.check_bounds(index, key) do
              result when is_inbound(result) ->
                disktable

              _ ->
                :not_found
            end

          :lower ->
            :not_found
        end

      m ->
        disktable = disktable(index: index) = element(m, disktables)

        case Disktable.check_bounds(index, key) do
          result when is_inbound(result) ->
            disktable

          :higher ->
            do_search_disktable_in_generation(disktables, key, m, r)

          :lower ->
            do_search_disktable_in_generation(disktables, key, l, m)
        end
    end
  end
end
