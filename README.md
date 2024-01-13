# Access DataBaseを制御するクラス

データベースをクラス内のpandasデータフレームにロードして使用するモードと（工事中）直接データベースを検索・アップデート・挿入するモードを予定。

## 使用方法

```Sample Internal DataFrame mode
from DataBaseCtrl import DataBaseCtrl, SerchCondition

# インスタンス、クラス内データフレームモード
DB = DataBaseCtrl('DataBase File Path', 'TableName')

# クラス内データフレームにデータベースを読み込む
res = DB.UpdateInternalDataFrame()
```

```Sample Direct DataBase access mode
# インスタンス、データベース直接アクセスモード（工事中）
DB = DataBaseCtrl('DataBase File Path', 'TableName', True)
```

その他、順次メソッドの説明を追加していきます。
