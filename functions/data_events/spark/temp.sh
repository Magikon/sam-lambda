#! /bin/zsh -

a="x"
b="y"
c="z"

if [ "$a" = "x" ] || [ "$b" = "5234" ] ; then 
	printf "okay .. yes !\n"
fi
# zmodload zsh/zutil
# zparseopts -A ARGUMENTS -cluster-name: -release-label:

# declare -a arr=("cluster-name" "release-label")
# printf '\n'
# ## now loop through the above array
# for i in "${arr[@]}"
# do
# 	# printf 'i is "%s" \n' "$i"
# 	arg=$ARGUMENTS[--$i]
# 	if [[ -n "$arg" ]]; then 
# 		v=${i/-/_}
# 		eval $v="$arg"
# 		printf 'Argument "%s" has value of "%s" \n' "$i" "$arg"
# 	fi 
# done


# printf 'cluster_name="%s"\n' "$cluster_name"
# printf 'release_label="%s"\n' "$release_label"
# for i in $arguments; 
# do
# 	# arg=$ARGUMENTS[--$i]
# 	# printf 'arg is "%s"\n' "$arg"
# done

# if [[ -n "$clusterName" ]]; then 
# 	printf 'Argument cluster-name is "%s"\n' "$clusterName"
# fi

# if [[ -n "$releaseLabel" ]] ; then
# 	printf 'Argument release-label is "%s"\n' "$releaseLabel"
# fi


# if [ -d "$MIRRORBALL_PATH/tmp" ]; then
#   # Control will enter here if $DIRECTORY exists.
#   echo "«tmp» folder exists"
# else 
#   mkdir $MIRRORBALL_PATH/tmp
# fi