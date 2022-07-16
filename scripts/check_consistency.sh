#!/bin/bash

GIDS="0 1 2"

echo "filtering logs..."
for f in out*.txt; do
    cat $f | grep DELIVERY > ${f%.*}.del
done


echo "limiting by smallest log..."
for g in $GIDS; do
    min=9999999999999999
    for f in out_${g}_*.del; do
        n=`cat $f | wc -l`
        if (( $n < $min )); then
            min=$n
        fi
    done
    # remove last line, may be broken
    min=$[min - 1]

    for f in out_${g}_*.del; do
        head -n $min $f > ${f%.*}.head
    done
done

echo "sorting and checking ts/id order..."
for f in out*.head; do
    cat $f | sort -k1,1g -k2,2g > ${f%.*}.sorted
    # check delivery order
    diff -q $f ${f%.*}.sorted
done

echo "checking replicas inside group are consistent..."
for g in $GIDS; do
    diff -q out_${g}_0.head out_${g}_1.head
    diff -q out_${g}_0.head out_${g}_2.head
done

echo "checking replicas are consistent across groups..."
# filter deliveries in g1 that include g2 in the destination
for g1 in $GIDS; do
    for g2 in $GIDS; do
        if [[ "$g1" != "$g2" ]]; then
            cat out_${g1}_0.head | grep "Gid($g2)" > out_${g1}_0.cross_${g2}
        fi
    done
done

# check order is the same
for g1 in $GIDS; do
    for g2 in $GIDS; do
        if (( $g1 < $g2 )); then
            # if some file is a prefix of the other, cmp stderr does not contain "differ"
            cmp out_${g1}_0.cross_${g2} out_${g2}_0.cross_${g1} 2>&1 |
                grep -q differ && echo "out_${g1}_0.cross_${g2} out_${g2}_0.cross_${g1} are not compatible"
        fi
    done
done
