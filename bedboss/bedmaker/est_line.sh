# Estimates the number of lines in a file

file=$1
nsample=1000
headbytes=`head -q -n $nsample $file | wc -c`
#tailbytes=`tail -q -n $nsample $file | wc -c`
#echo $headbytes

filesize=`ls -sH --block-size=1 $file | cut -f1 -d" "`
#echo $filesize

echo -n $((filesize / (headbytes) * $nsample))
#echo " (" $((filesize / headbytes )) "K;" $((filesize / headbytes /1000 )) "M )"
