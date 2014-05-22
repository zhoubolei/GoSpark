#!/bin/bash
# ssh-multi
# to ssh to multiple servers

starttmux() {
#local hosts=(vision24 vision28 vision35)
local hosts=(vision25 vision26 vision27 vision28 vision29 vision30 vision31 vision32 vision33 vision34 vision35 vision36 vision37 vision38)
local UNAME=XXX # removed for security reason
 
tmux new-window "ssh $UNAME@${hosts[0]}"
unset hosts[0];
for i in "${hosts[@]}"; do
tmux split-window -h "ssh $UNAME@$i"
tmux select-layout tiled > /dev/null
done
tmux select-pane -t 0
tmux set-window-option synchronize-panes on > /dev/null
 
}
 
HOSTS=${HOSTS:=$*}
 
starttmux
