curl -O https://s3.amazonaws.com/mlsys-artifact/fever.csv
curl -O https://s3.amazonaws.com/mlsys-artifact/squad.csv
curl -O https://s3.amazonaws.com/mlsys-artifact/fever_reordered.csv
curl -O https://s3.amazonaws.com/mlsys-artifact/fever_with_evidence_5.csv

mv fever.csv squad.csv ./datasets/
mv fever_with_evidence_5.csv fever_reordered.csv ./run/accuracy/datasets/