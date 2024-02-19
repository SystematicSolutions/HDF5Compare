rem
rem  RunHDF5Comparison.cmd
rem
rem 
     julia --threads 25 --project DatabaseCompare.jl "Base" "C:\2020JuliaBeta_Main\2020Model\11.01 420 Base\database.hdf5" "StartBase" "C:\2020JuliaBeta_Main\2020Model\StartBase\database.hdf5"
     Pause