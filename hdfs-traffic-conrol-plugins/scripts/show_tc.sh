#/bin/sh
DEV=$1
echo sudo tc qdisc show dev $DEV
sudo tc qdisc show dev $DEV
echo sudo tc class show dev $DEV
sudo tc class show dev $DEV
echo sudo tc filter show dev $DEV
sudo tc filter show dev $DEV

