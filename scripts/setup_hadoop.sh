#! /bin/bash
echo Sourcing LCG view LCG_105a_swan
source /cvmfs/sft.cern.ch/lcg/views/LCG_105a_swan/x86_64-el9-gcc13-opt/setup.sh

echo Sourcing Hadoop
cd /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext && source hadoop-swan-setconf.sh hadoop-analytix && cd -

echo Setting up Kerberos
export KRB5CCNAME=FILE:$XDG_RUNTIME_DIR/krb5cc
kinit
