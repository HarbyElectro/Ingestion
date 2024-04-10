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

# Data Pipelines

![image](https://github.com/HarbyElectro/Ingestion/assets/152432979/5b5f793f-d2d9-4f84-9429-e6e0862cdfbc)

SmartIngest © 2024 by Ahmed Harby is licensed under CC BY 4.0 
