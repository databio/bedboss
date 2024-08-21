#!/bin/bash
cp ../docs/templates/usage.template usage.template
# bedboss --help > USAGE.temp 2>&1

for cmd in "--help" "check-requirements --help" "delete-bed --help" "delete-bedset --help" "init-config --help" "make-bed --help" "make-bedset --help" "make-bigbed --help" "reindex --help" "run-all --help" "run-pep --help" "run-qc --help" "run-stats --help"; do
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