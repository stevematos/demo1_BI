
# DEMO 1

Comando para correr el dataflow
```shell
python main.py -i gs://storage-fifa/valores_players.csv  --project hardy-scarab-253003  
// --runner DataflowRunner   --temp_location     gs://storage-fifa/     
// --output     gs://storage-fifa/data_final/players_final.txt   --setup_file ./setup.py  --job_name dataflowdem1
```