#!/bin/bash
die() {
  printf '%s\n' "$1" >&2
  exit 1
}

bkp_name="bkp-$$"

data_dir="/var/lib/cassandra/data/"
cqlsh_host="localhost"

while :; do
  case $1 in
    -d|--data-dir)
      if [ "$2" ]; then
        data_dir=$2
        shift
      else
        die 'ERROR: "--data-dir" requires a non-empty option argument.'
      fi
    ;;
    -h|--host)
      if [ "$2" ]; then
        cqlsh_host=$2
        shift
      else
        die 'ERROR: "--host" requires a non-empty option argument.'
      fi
    ;;
    -k|--keyspace)
      if [ "$2" ]; then
        if [ "$keyspace" ]; then
          keyspace=$2
        else
          die 'ERROR: "--keyspace" multiple occurrences'
        fi
        shift
      else
        die 'ERROR: "--keyspace" requires a non-empty option argument.'
      fi
    ;;
    --help)
      echo -e "Usage: export.sh [OPTION]...\n"
      echo "export.sh makes a backup (snapshot) of a remote cassandra cluster's keyspace and"
      echo "rsync it to local machine. It is best used incrementally, letting rsync only get"
      echo -e "incremental changes.\n"
      echo "  -d, --data-dir  Cassandra data dir (default: /var/lib/cassandra/data/)"
      echo "  -h, --host      Cassandra's host   (default: localhost)"
      echo "  -k, --keyspace  Keyspace to export"
      echo -e "  --help          Shows this help message\n"
      exit 0
    ;;
    *)
    break
  esac
  shift
done

if [ -z "${keyspace}" ]; then
    die 'ERROR: "--keyspace" must be supplied'
fi

echo "Create snapshot named [${bkp_name}] at host [${cqlsh_host}] in data dir [${data_dir}] for keyspace [${keyspace}]"
nodetool snapshot "${keyspace}" -t "${bkp_name}"

echo "Preparing backup file"
for name in $(find  "${data_dir}/${keyspace}/"*"/snapshots/${bkp_name}" -type f); do
        new=$(echo "$name" | sed -e "s#${data_dir}/##g" -e "s#\([^/]\+\)/\([^-]\+\).\+/snapshots/${bkp_name}/\([^/]\+\)\$#\1/\2/\3#g")
        mkdir -p "${bkp_name}/$(dirname $new)"
        cp -al "$name" "${bkp_name}/$new"
done

echo "Remove snapshot named: ${bkp_name}"
nodetool clearsnapshot -t "${bkp_name}" "${keyspace}"

echo "Dump keyspace and table creation instruction"
cqlsh -e "desc \"${keyspace}\";" $cqlsh_host > "${bkp_name}/${keyspace}.sql"

# TODO: maybe provide an option for the user to choose if to compress or leave uncompressed which is easier to rsync

echo "Create tar file: ${keyspace}.tar.gz"
cd "${bkp_name}"
tar --use-compress-program=pigz -cf "../${keyspace}.tar.gz" .
cd -

echo "Remove temporary files"
rm -rf "${bkp_name}"
