# Ingestion

# Requirements
  * Python 3.5
  * Apache Spark (Pyspark Python) version 3.3.2
  * Delta LH (delta python) library, version 0.4.2
  * Confluent Kafka 2.3.0 Python library
  * Pyflink 1.9 Python library
  
  
# DataSets
* MobiAct: We use the MobiAct dataset, which is a publicly available dataset of accelerometer and gyroscope sensor readings collected from mobile devices during various physical activities. The dataset contains sensor data for activities such as walking, jogging, sitting, standing, and ascending and descending stairs, among others. To collect the data, we will download the MobiAct dataset from the source website and extract the relevant sensor data.

* IMDb: We opted to use the IMDb database‎ instead of a synthetic data set, as it contains extensive information on movies, actors, directors, and production companies. The dataset at hand is rather intricate, occupying a considerable amount of storage, amounting to 5.34 GB in TSV format. The complexity of the dataset may pose a challenge to its processing and analysis.

* CelebA: CelebA is a large-scale dataset with over 200,000 celebrity images and 40 attribute labels describing facial characteristics and features. The dataset is diverse, with people from different backgrounds, genders, and age groups. CelebA has been used for various tasks, including facial attribute prediction, face detection and recognition, and generative model training.

* UCF101: The UCF101 dataset contains over 13320 realistic action videos from 101 categories, making it the most diverse action recognition dataset available. Unlike other datasets, UCF101 features realistic representations of actions, offering additional context through 25 distinct groups. The dataset is challenging due to variations in camera motion, object appearance and pose, object scale, viewpoint, cluttered background, and illumination conditions. UCF101 is a unique and invaluable resource for advancing the field of action recognition. The action categories can be divided into five types: a) Human-Object Interaction, b) Body-Motion Only 3) Human-Human Interaction 4) Playing Musical Instruments 5) Sports. However, ingesting video data is a complex process that varies depending on the application and use case. By ingesting the UCF101 dataset, researchers and developers can rely on complex video datasets to create, train, and test algorithms, models, and systems in challenging real-world scenarios.

# Train GraphTransformer
```
 python TrainGT.py --enc_layers=8 --dec_layers=6 --num_heads=2 --num_units=256 --emb_dim=300  --train_dir=save/ --use_copy=1 --batch_size=16 --dropout_rate=0.2 --gpu_device=0 --max_src_len=90 --max_tgt_len=90
 ```
# Implemenation

* 

     

* 

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
