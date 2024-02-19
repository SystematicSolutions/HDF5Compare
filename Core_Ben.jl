#import Pkg; Pkg.add("HDF5") # Pkg.add("DataFrames") # Pkg.add("CSV") # Pkg.add("Printf")
using HDF5, DataFrames, CSV, Printf, Base.Threads;

function OpenFile(fileName, readWrite)
    open(fileName, readWrite)
end
function WriteFileHeader(fileToWrite, fileName)
    write(fileToWrite,"*\n")
    write(fileToWrite,"* $(fileName)\n")
    write(fileToWrite,"*\n")
end
function ReadDisk(db::String, name::String)
    arr = h5open(db, "r") do f
            read(f[name])
    end
    arr
end

function ReadDisk(::Type{DataFrame}, db::String, name::String; skip_zeros = false)
  h5open(db, "r") do f
    !haskey(f, name) && throw(HDF5DataSetNotFoundException(db, name))
    dataset = f[name]
    attr = Dict(attrs(dataset))
    get(attr, "type", "") != "variable" && error("`ReadDisk(DataFrame, db, \"$name\")` not supported for sets")
    function g(name, dim)
      out = read(f["$(dirname(name))/$dim"])
      n = length(out)
      if first(out) == "" 
        out = 1:n
      end
      return out
    end
    dims = [Symbol(dim) => collect(g(name,dim)) for dim in attr["dims"]]
    units = attr["units"]
    arr = read(dataset)
    df = allcombinations(DataFrame, dims...)
    df[!, :Value] = reshape(arr, (prod(size(arr)),))
    if "Year" in names(df)
      df.Year = parse.(Int, df.Year)
    end
    metadata!(df, "variable", basename(name); style = :note)
    metadata!(df, "group", dirname(name); style = :note)
    metadata!(df, "name", name; style = :note)
    metadata!(df, "units", isempty(units) ? missing : units; style = :note)
    if skip_zeros
      subset!(df, :Value => ByRow(!isapprox(0.0)))
    end
    df
  end
end

function output_df(db, name; skip_zeros = true, parse_year = true, row_index = true)
  df = ReadDisk(DataFrame, db, name; skip_zeros)
  if row_index
    df.id = 1:nrow(df)
    select!(df, :id, Not(:id))
  elseif "id" in names(df)
    select!(df, Not(:id))
  end
  rename!(df, :Value => basename(name))
  if !parse_year
    df.Year = String.(df.Year)
  end
  df
end


function BuildVariableList(dbFileToRead)
  variables = Dict()
  h5open(dbFileToRead) do f
    for group in f
      for dataset in group
        attr = Dict(attrs(dataset))
        if "type" in keys(attr) && attr["type"] == "variable"
          dsName=string(lstrip(HDF5.name(dataset), '/'))
          variables[dsName] = [Symbol(dim) => collect(read(group["$dim"])) for dim in attr["dims"]]
        end
      end
    end
  end  
  variables
end


