#!/bin/sh

CLUSTER_MACHINES="braque cezanne chagall corot dali degas ernst gauguin leger leonardo magritte matisse millet miro modigliani monet pissarro renoir vangogh"

while true; do
	for machine in $CLUSTER_MACHINES; do
		ssh $machine ls&
	done;
	sleep 300 # 60 secondes
done

