defmodule Microlsm.Fs do
  @moduledoc false
  # Wrapper around :prim_file

  @funcs [
    close: 1,
    datasync: 1,
    delete: 1,
    open: 2,
    position: 2,
    pread: 3,
    pwrite: 3,
    rename: 2,
    sync: 1,
    truncate: 1,
    write: 2
  ]

  require ODCounter

  def init_counters do
    ODCounter.new(Microlsm, __MODULE__, ignore_if_exists: true)
  end

  def stats do
    ODCounter.to_map(Microlsm, __MODULE__)
  end

  for {name, arity} <- @funcs do
    def unquote(name)(unquote_splicing(Macro.generate_arguments(arity, __MODULE__))) do
      ODCounter.add(Microlsm, __MODULE__, unquote(name))
      :prim_file.unquote(name)(unquote_splicing(Macro.generate_arguments(arity, __MODULE__)))
    end
  end
end
