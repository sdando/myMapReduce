awk '
{
    var1="sed '\''s/,"
    var2="./"
    var3="/g'\'' -i $1"
    print var1 $1 var2 var3
}' $1
