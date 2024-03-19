# run locally
```
pipenv shell
python main.py
```

# ssh
Get from Oliver
```
ssh root@<ip>
```

# start / stop scheduler
```
systemctl start trackuriboh.service
systemctl stop trackuriboh.service
```

# see logs
```
systemctl status trackuriboh.service
```

# get into psql
```
psql -U postgres -h localhost
\c trackuriboh_db
\dt
```