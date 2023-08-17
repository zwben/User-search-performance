## README

### Characterizing and Early Predicting User Performance for Adaptive Search Path Recommendation

This repository contains a consolidated script of a paper published in ASIST2023 for processing and analyzing data from four datasets: SearchSuccess, KDD19, SIGIR16 (extension of KDD19), and TREC14-Session track.

#### Requirements:

- Python 3.x
- Pandas
- Matplotlib
- Seaborn
- Scipy
- sklearn
- bs4
- Tensorflow

#### Usage:

1. Ensure you have the datasets downloaded and placed in the appropriate directory. Adjust the file paths in the script if necessary.
2. Run .py file to extract features from the corresponding dataset.

3. Open .ipynb file in Jupyter Notebook or Google Colab to process the data, train the model interactively.

#### Datasets:

The datasets can be found at the following links:

- [SearchSuccess Dataset](http://www.thuir.cn/data-SearchSuccess/)
- [KDD19 Dataset](http://www.thuir.cn/KDD19-UserStudyDataset/)
- [SIGIR16 Dataset](https://github.com/THUIR/UsefulnessUserStudyData) (KDD19 extension)
- [TREC14-Session track Dataset](https://trec.nist.gov/data/session2014.html)

#### Publication:

If you use this code, please cite our publication:

```latex
@article{wang2023,
author = {Wang, Ben and Liu, Jiqun},
title = {Characterizing and Early Predicting User Performance for Adaptive Search Path Recommendation},
journal = {Proceedings of the Association for Information Science and Technology},
keywords = {User performance, Web search; search path recommendation, evaluation, proactive information retrieval},
year = {2023}
}
```

For any issues or questions regarding the code, please [contact us](mailto:benw@ou.edu). 
