#! /bin/bash
#
# This is a script for smoke-testing a patch before submitting it for an Evergreen run. Allows the
# patch to compile and execute faster and to weed out simple programming errors without wasting AWS
# time and money.
#
# Export a patch of the changes though `git format-patch` into a file:
#  > git format-patch --stdout <Git hash> > Patch.patch
#
# Smoke the patch using the following command line:
#  > smoke_patch.sh ~/Patch.patch
#

export PATCHFILE=$1

echo "Using patch file $PATCHFILE"
if [ ! -f $PATCHFILE ]; then
    echo "File $PATCHFILE not found"
    exit 1
fi

export TESTRUNDIR=/tmp/TestRunDirectory

echo "Using test run directory $TESTRUNDIR"
if [ -d $TESTRUNDIR ]; then
    echo "Deleting previous test run directory $TESTRUNDIR ..."
    rm -rf $TESTRUNDIR
fi

mkdir "$TESTRUNDIR"

export TESTDBPATHDIR="$TESTRUNDIR/db"
mkdir "$TESTDBPATHDIR"

export TOOLSDIR=/home/kaloianm/mongodb/4.0.0

export RESMOKECMD=buildscripts/resmoke.py
export SCONSCMD=buildscripts/scons.py

export CPUS_FOR_BUILD=500
export CPUS_FOR_LINT=6
export CPUS_FOR_TESTS=12

export MONGO_VERSION_AND_GITHASH="MONGO_VERSION=0.0.0 MONGO_GIT_HASH=unknown"

if [ "$2" == "dynamic" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl --link-model=dynamic CC=`which clang-3.8` CXX=`which clang++-3.8`"
elif [ "$2" == "clang" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl CC=`which clang` CXX=`which clang++`"
elif [ "$2" == "clang-3.8" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl CC=`which clang-3.8` CXX=`which clang++-3.8`"
elif [ "$2" == "ubsan" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl --allocator=system --sanitize=undefined,address CC=`which clang` CXX=`which clang++`"
elif [ "$2" == "opt" ]; then
    export FLAGS_FOR_BUILD="--dbg=off --opt=on --ssl"
else
    export FLAGS_FOR_BUILD="--dbg=on --opt=off --ssl"
fi

export BUILD_NINJA_CMDLINE="$SCONSCMD $FLAGS_FOR_BUILD $MONGO_VERSION_AND_GITHASH --icecream VARIANT_DIR=ninja build.ninja"
export BUILD_CMDLINE="ninja -j $CPUS_FOR_BUILD all"

export LINT_CMDLINE="$SCONSCMD -j $CPUS_FOR_LINT $FLAGS_FOR_BUILD $MONGO_VERSION_AND_GITHASH --no-cache --build-dir=$TESTRUNDIR/mongo/lint lint"

export FLAGS_FOR_TEST="--dbpathPrefix=$TESTDBPATHDIR --continueOnFailure --log=file"

git clone --depth 1 git@github.com:mongodb/mongo.git "$TESTRUNDIR/mongo"
pushd "$TESTRUNDIR/mongo"

git clone --depth 1 git@github.com:RedBeard0531/mongo_module_ninja.git "src/mongo/db/modules/ninja"

#
# TODO: Support for Enterprise builds
#
if false; then
    echo "Cloning the enterprise repository ..."
    git clone --depth 1 git@github.com:10gen/mongo-enterprise-modules.git 'src/mongo/db/modules/subscription'
fi

#
# TODO: Support for RocksDB builds
#
if false; then
    echo "Cloning the RocksDB repository ..."
    git clone --depth 1 git@github.com:mongodb-partners/mongo-rocks.git 'src/mongo/db/modules/rocksdb'
fi

echo "Applying patch file $PATCHFILE"
git am $PATCHFILE
if [ $? -ne 0 ]; then
    echo "git apply failed with error $?"
    exit 1
fi

#
# Start the slower builder and linter first so that the slower tasks can overlap with it
#
echo "Starting build ninja and lint ..."

echo "Command lines:" > build.log
echo $BUILD_NINJA_CMDLINE >> build.log
time $BUILD_NINJA_CMDLINE >> build.log 2>&1 &
PID_build_ninja=$!

echo "Command lines:" > lint.log
echo $LINT_CMDLINE >> lint.log
time $LINT_CMDLINE >> lint.log 2>&1 &
PID_lint=$!

#
# Copy any binaries which are needed for running tests
#
echo "Copying executables to support tests ..."
cp "$TOOLSDIR/mongodump" `pwd`
cp "$TOOLSDIR/mongorestore" `pwd`

echo "Waiting for build ninja ..."
wait $PID_build_ninja
if [ $? -ne 0 ]; then
    echo "build  ninja failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

echo "Starting build ..."
echo $BUILD_CMDLINE >> build.log
time $BUILD_CMDLINE >> build.log 2>&1 &
PID_build=$!

#
# Wait for the build and linter to complete
#
echo "Waiting for build ..."
wait $PID_build
if [ $? -ne 0 ]; then
    echo "build failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

echo "Waiting for lint ..."
wait $PID_lint
if [ $? -ne 0 ]; then
    echo "lint failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

#
# Execute the unit tests and dbtest first to uncover early problems, before even scheduling any of
# the longer running JS tests
#
echo "Running WT unittests ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --suites=unittests
if [ $? -ne 0 ]; then
    echo "unittests failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

echo "Running WT dbtest ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=dbtest
if [ $? -ne 0 ]; then
    echo "WT dbtest failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

echo "Running WT core ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=core
if [ $? -ne 0 ]; then
    echo "WT basic tests failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

#
# Aggregation tests (they run relatively quick and uncover early sharding problems)
#
echo "Running WT aggregation ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=aggregation
if [ $? -ne 0 ]; then
    echo "WT aggregation failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

#
# Auth tests
#
echo "Running WT auth ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=auth
if [ $? -ne 0 ]; then
    echo "WT auth failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

#
# Sharding jscore passthough
#
echo "Running WT sharding_jscore_passthrough ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=sharding_jscore_passthrough
if [ $? -ne 0 ]; then
    echo "WT sharding_jscore_passthrough failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

#
# Sharding suite
#
echo "Running WT sharding ..."
time $RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=sharding
if [ $? -ne 0 ]; then
    echo "WT sharding failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi
