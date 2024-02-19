#import Pkg; Pkg.add("HDF5") # Pkg.add("DataFrames") # Pkg.add("CSV") # Pkg.add("Printf")
using HDF5, DataFrames, CSV, Printf, Base.Threads;

include("Core_Ben.jl") 

if length(ARGS) >0
  scenarioName = ARGS[1]
  databasePath = ARGS[2]
  compScenarioName = ARGS[3]
  compDbPath = ARGS[4]
else
  scenarioName = "Base"
  databasePath = "C:/HDF5Compare/Base/database.hdf5"
  compScenarioName =  "StartBase" 
  compDbPath = "C:/HDF5Compare/StartBase/database.hdf5"
end

println(scenarioName, " $scenarioName")
println(databasePath, " $databasePath")
println(compScenarioName, " $compScenarioName")
println(compDbPath, " $compDbPath")

function VaribleShouldBeCompared(new, variableName)
  if typeof(new) != Vector{String}
    if typeof(new) != String
      if variableName !== "CgUnCode" 
        if variableName !== "F1New"
          return true
        end  
      end
    end            
  end
end

function BuildComparisonDataFrame(new_db,old_db,variableName,key)
  println("Calculating differences for $variableName")
  new_df = output_df(new_db, key; skip_zeros = false)
  old_df = output_df(old_db, key; skip_zeros = false)
  new_df[!, "$variableName" * "_$compScenarioName"] = old_df[!, variableName]
  rename!(new_df, variableName => "$variableName" * "_$scenarioName")
  comparisonDf = subset(new_df, ["$variableName" * "_$scenarioName", "$variableName" * "_$compScenarioName"] => (new, old) -> (!).(isapprox.(new, old, rtol = 0.05)))
  comparisonDf
end

function AddDifferenceColumns(comparisonDf,variableName)
  comparisonDf = select!(comparisonDf,Not(:id))
  println("Add Diff for $variableName")
  vaulesColName = "$variableName" * "_$scenarioName"
  compValuesColName = "$variableName" * "_$compScenarioName"
  diffCol = comparisonDf[!,"$vaulesColName"] .- comparisonDf[!,"$compValuesColName"]
  comparisonDf[!,:"Difference"] = diffCol
  percentDiffCol = comparisonDf[!,:Difference] ./  comparisonDf[!,"$vaulesColName"]
  comparisonDf[!,:"PercDifference"] = percentDiffCol
  comparisonDf=sort!(comparisonDf, :PercDifference, rev=true)
  comparisonDf
end

function CompareVariables(new_db, old_db, key)
  vNameSlashDbName = String.(split(key, '/'))
  variableName = vNameSlashDbName[2]
  dbName = vNameSlashDbName[1]
  if dbName == "MOutput"
    new = ReadDisk(new_db, key)
    old = ReadDisk(old_db, key)
    if VaribleShouldBeCompared(new, variableName)==true
      if count(==(false), isapprox.(new, old)) > 0
        comparisonDf =  BuildComparisonDataFrame(new_db,old_db,variableName,key)
        if !isempty(comparisonDf)
          comparisonDf = AddDifferenceColumns(comparisonDf,variableName)
          filename = variableName * dbName
          CSV.write("C:/HDF5Compare/Output/$filename" * "Compare.CSV",comparisonDf)
        end
      end
    end
  end
end

function CompareDatabases(new_db, old_db)
  variables = BuildVariableList(new_db)
  varList = collect(keys(variables))
  varCount = length(varList)
  println(varCount, " varCount")
  Threads.@threads for i in 1:varCount
    currentVar = varList[i]
    CompareVariables(new_db,old_db,currentVar)
  end
end

CompareDatabases(databasePath, compDbPath)
println("HDF5 DB Comparison Complete!")


