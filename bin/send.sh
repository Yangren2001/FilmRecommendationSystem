#!/bin/bash

exec 3<>/dev/"tcp"/hadoop103/6666
echo `cat ./src/log/test.log`>&3
exec 3<&-