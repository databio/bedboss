#!/bin/bash
cp ../docs/templates/usage.template usage.template
# bedboss --help > USAGE.temp 2>&1
commands=$(bedboss get-commands)

commands_with_help=()
for cmd in $commands; do
  commands_with_help+=("$cmd --help")
done

for cmd in "--help" "geo --help" "geo upload-all --help" "geo upload-gse --help" "${commands_with_help[@]}"; do
	echo $cmd
	echo -e "## \`bedboss $cmd\`" > USAGE_header.temp
	bedboss $cmd --help > USAGE.temp 2>&1
	# sed -i 's/^/\t/' USAGE.temp
	sed -i.bak '1s;^;\`\`\`console\
;' USAGE.temp
#	sed -i '1s/^/\n\`\`\`console\n/' USAGE.temp
	echo -e "\`\`\`\n" >> USAGE.temp
	#sed -i -e "/\`looper $cmd\`/r USAGE.temp" -e '$G' usage.template  # for -in place inserts
	cat USAGE_header.temp USAGE.temp >> usage.template # to append to the end
done
rm USAGE.temp
rm USAGE_header.temp
rm USAGE.temp.bak
mv usage.template ../docs/usage.md
#cat usage.template
# rm USAGE.temp