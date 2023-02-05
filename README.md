# inventar
pushes contents of a file system to data list within your elastic search

# install

```
git clone git@github.com:santex/inventar.git
cd inventar;
python3 -m pip install -r requirements.txt;
```

# run

python3.9 run.py --src /tmp/data  --threads 4



# data

a sample entry for each file 

```
{
    "file_tag": "1675547479_44626985a36cf6dd0d625e42eb91062e",
    "file_name": "published.csv",
    "full_path": "../index2/app/data/published.csv",
    "md5": "44626985a36cf6dd0d625e42eb91062e",
    "size": 276,
    "stats": [
      33188,
      37972922,
      16777220,
      1,
      502,
      20,
      276,
      1675547481,
      1675547479,
      1675547479
    ],
    "type": "private",
    "ext": ".csv",
    "create_time": "2023-02-04 22:51:19.816506",
    "modify_time": "2023-02-04 22:51:19.816506",
    "tags": [
      "csv",
      "text"
    ],
    "info_time": "2023-02-05 12:57:39.055488",
    "data": {
      "rows": 3
    }
  }

``
