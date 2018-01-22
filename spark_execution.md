# Temps d'exécution et paramètres optimaux

https://stackoverflow.com/questions/37871194/how-to-tune-spark-executor-number-cores-and-executor-memory
https://stackoverflow.com/questions/24622108/apache-spark-the-number-of-cores-vs-the-number-of-executors

## Première exécution

```
spark-submit --master yarn --deploy-mode cluster --executor-memory 7G --num-executors 50 ~/espaces/travail/Master_2/Programmation\ Large\ Echelle/PLE_DLM/src/stats/target/PLE_DLM-0.0.1.jar
```

Temps d'exécution

```
real	4m55,279s
user	0m17,128s
sys	0m2,596s
```

## Seconde exécution

Executors 118
Cores 4

```
spark-submit --master yarn --deploy-mode cluster --executor-memory 7G --num-executors 118 --executor-cores 4 ~/espaces/travail/Master_2/Programmation\ Large\ Echelle/PLE_DLM/src/stats/target/PLE_DLM-0.0.1.jar
```

Temps d'exécution

```
real	4m9,056s
user	0m16,196s
sys	0m2,436s
```

# Fonctionnement de HBase

https://goranzugic.wordpress.com/2016/04/11/hbase-schema/
http://www.informit.com/articles/article.aspx?p=2253412
