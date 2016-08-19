# oss-sync

现在好用的能同步的网盘都没有了，于是自己用阿里云的OSS撸了一个

### 用法

命令:

    python oss-sync.py <配置文件名>

配置文件实例: (JSON格式)

    {
        "access_key_id": "",                        // 阿里云的ACCESS_KEY_ID
        "access_key_secret": "",                    // 阿里云的ACCESS_KEY_SECRET
        "bucket": "chishaxie-sync",                 // 用于同步的OSS的Bucket(桶)
        "endpoint": "oss-cn-beijing.aliyuncs.com",  // 该Bucket(桶)所在的endpoint(地域)
        "local": "G:\\sync",                        // 本地的同步根路径
        "remote": "sync"                            // 远端Bucket(桶)下面的同步根路径
    }
