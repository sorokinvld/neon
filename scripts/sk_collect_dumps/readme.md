# Collect /v1/debug_dump from all safekeeper nodes

1. Update the inventory, if needed
2. Run ansible playbooks to collect .json dumps from all safekeepers and store them in `./result` directory.
3. Run `DB_CONNSTR=... ./upload.sh prod_feb30` to upload dumps to `prod_feb30` table in specified postgres database.

## How to update the inventory

Get a list of all staging safekeepers:
```
tsh ls | awk '{print $1}' | grep safekeeper | grep "neon.build" > hosts
```

## Run ansible

Test connection:
```
ansible all -m ping -v
```

Download the dumps:
```
mkdir result
ansible-playbook remote.yaml
```

