
#!/bin/bash
tar_file=$1
bkp_name="bkp-$$"
cqlsh_host=${CQLSH_HOST:-"localhost"}

if [ -z "${tar_file}" ]; then
    echo "Usage import.sh [tar file]"
    exit 1
fi

keyspace=$(basename "${tar_file}" ".tar.gz")

mkdir -p "${bkp_name}"

tar --use-compress-program=pigz -xvf "${tar_file}" -C "${bkp_name}"

echo "Drop keyspace ${keyspace}"
cqlsh -e "drop keyspace \"${keyspace}\";"

echo "Create empty keyspace: ${keyspace}"
cat "${bkp_name}/${keyspace}.sql" | cqlsh

for dir in "${bkp_name}/${keyspace}/"*; do
    sstableloader -d ${cqlsh_host} "${dir}"
done
