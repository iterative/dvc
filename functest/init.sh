
source common.rc

./clean.sh

rm -rf $RAW_DATA_LOCAL
mkdir $RAW_DATA_LOCAL
for file in "Badges.xml" "Tags.xml" "PostLinks.xml"
do
    (cd $RAW_DATA_LOCAL; wget $RAW_DATA_S3/$file)
done

rm -rf $CODE_LOCAL
mkdir $CODE_LOCAL
(cd $CODE_LOCAL; wget "$BASE_DIR_S3/functests/code/xmltotsv.py")

