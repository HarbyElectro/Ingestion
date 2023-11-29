# Ingestion

# Requirements
  * Python 3.5
  * Apache Spark (Pyspark Python) version 3.3.2
  * 
  
  
# DataSets
* MobiAct
We use the MobiAct dataset, which is a publicly available dataset of accelerometer and gyroscope sensor readings collected from mobile devices during various physical activities. The dataset contains sensor data for activities such as walking, jogging, sitting, standing, and ascending and descending stairs, among others. To collect the data, we will download the MobiAct dataset from the source website and extract the relevant sensor data.

*IMDb
We opted to use the IMDb database‎ instead of a synthetic data set, as it contains extensive information on movies, actors, directors, and production companies. The dataset at hand is rather intricate, occupying a considerable amount of storage, amounting to 5.34 GB in TSV format. The complexity of the dataset may pose a challenge to its processing and analysis.


# Train GraphTransformer
```
 python TrainGT.py --enc_layers=8 --dec_layers=6 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
 ```
# Test GraphTransformer
```
python TestGT.py --enc_layers=8 --dec_layers=6 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
```
# Train DCGCN
```
python Train(DCGCN).py --enc_layers=8 --dec_layers=6 --gcn_num_layers=4 --gcn_dropout=0.5 --gcn_num_hidden=16 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
```
# Test DCGCN
```
python Test(DCGCN).py --enc_layers=8 --dec_layers=6 --gcn_num_layers=4 --gcn_dropout=0.5 --gcn_num_hidden=16 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
```
# Train LDGCN
```
python Train(LDGCN).py --enc_layers=8 --dec_layers=6 --gcn_num_layers=4 --gcn_dropout=0.5 --gcn_num_hidden=16 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
```
# Test LDGCN
```
python Test(LDGCN).py --enc_layers=8 --dec_layers=6 --gcn_num_layers=4 --gcn_dropout=0.5 --gcn_num_hidden=16 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
```
# Implemenation

Zhang, Y., Guo, Z., Teng, Z., Lu, W., Cohen, S. B., Liu, Z., & Bing, L. (2020). Lightweight, Dynamic Graph Convolutional Networks for AMR-to-Text Generation. arXiv preprint arXiv:2010.04383.

* DCGCN: model design is mainly based on this approach which is described in <a href="https://github.com/yanzhangnlp/LDGCNs"> LDGCN </a> .

     ![image](https://user-images.githubusercontent.com/77679146/114119100-04640400-98b8-11eb-8312-df203d463d81.png)

* LDGCN: model is designed based on the approach provided in the LDGCN model for both layerwise and depthwise aspects.

     ![image](https://user-images.githubusercontent.com/77679146/114119206-34aba280-98b8-11eb-9b41-3e2a39a56901.png)
     ![image](https://user-images.githubusercontent.com/77679146/114119220-3bd2b080-98b8-11eb-9a4e-5ad98c285112.png)
* For implementation of the models, convolution-block is added to the code of both models.
* Due to hardware limiations LSTM decoding couldn't be used but it's already implemented.

# Evaluation and Resutls
* The checkpoints of the training epochs are dumped to the "save" folder due to the limitation on the size of the epochs checkpoints. It cannot be uploaded.
* For model evaluation, the resutls of expermiemnts are in the "results" folder.
* The results folder contains the log file of the expermients alongside with the reference and prediction files for each experiment.

```
python Evaluation.py
```
