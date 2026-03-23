defmodule Microlsm.Fs do
  @funcs [
    close: 1,
    datasync: 1,
    delete: 1,
    open: 2,
    pread: 3,
    pwrite: 3,
    rename: 2,
    sync: 1,
    truncate: 1,
    write: 2
  ]

  for {name, arity} <- @funcs do
    defdelegate unquote(name)(unquote_splicing(Macro.generate_arguments(arity, __MODULE__))), to: :prim_file
  end
end
