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

export MONGODBTOOLCHAIN="/opt/mongodbtoolchain/v2/bin"
export PATH=$MONGODBTOOLCHAIN:$PATH
export RESMOKECMD="python buildscripts/resmoke.py"
export SCONSCMD="python buildscripts/scons.py"

export CPUS_FOR_BUILD=640
export CPUS_FOR_LINT=12
export CPUS_FOR_TESTS=12

export MONGO_VERSION_AND_GITHASH="MONGO_VERSION=0.0.0 MONGO_GIT_HASH=unknown"

if [ "$2" == "dynamic" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl --link-model=dynamic CC=`which clang` CXX=`which clang++`"
elif [ "$2" == "clang" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl CC=`which clang` CXX=`which clang++`"
elif [ "$2" == "clang-3.8" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl CC=`which clang-3.8` CXX=`which clang++-3.8`"
elif [ "$2" == "ubsan" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl --allocator=system --sanitize=undefined,address CC=`which clang` CXX=`which clang++`"
elif [ "$2" == "opt" ]; then
    export FLAGS_FOR_BUILD="--dbg=off --opt=on --ssl"
elif [ "$2" == "dbg" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=off --ssl"
else
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl"
fi

export FLAGS_FOR_TEST="--dbpathPrefix=$TESTDBPATHDIR --continueOnFailure --log=file"

# Construct the scons, ninja and linter command lines
export BUILD_NINJA_CMDLINE="$SCONSCMD $FLAGS_FOR_BUILD $MONGO_VERSION_AND_GITHASH --icecream VARIANT_DIR=ninja build.ninja"
export BUILD_CMDLINE="ninja -j $CPUS_FOR_BUILD all"
export LINT_CMDLINE="$SCONSCMD -j $CPUS_FOR_LINT $FLAGS_FOR_BUILD $MONGO_VERSION_AND_GITHASH --no-cache --build-dir=$TESTRUNDIR/mongo/lint lint"

# Clone the repositories
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

# Apply the patch to be run
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

echo "Waiting for build ninja process $PID_build_ninja to complete ..."
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
echo "Waiting for build process $PID_build to complete ..."
wait $PID_build
if [ $? -ne 0 ]; then
    echo "build failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

##################################################################################################
# Function to execute a given test suite and block until it completes. Terminates the script if an
# error is reported from the test execution.
execute_test_suite () {
    echo "Running suite $1 with command line $2 ..."

    # Ensure that lint has completed
    if ! kill -0 $PID_lint > /dev/null 2>&1; then
        echo "Waiting for lint process $PID_lint to complete ..."
        wait $PID_lint; local TIMEOUT_RESULT=$?
        if [ $TIMEOUT_RESULT -ne 0 ]; then
            echo "lint failed with error $TIMEOUT_RESULT"
            kill -9 `jobs -p`
            exit 1
        fi
    fi

    # Kick off the actual test suite
    (time $2); local SUITE_RESULT=$?
    if [ $SUITE_RESULT -ne 0 ]; then
        echo "Suite $1 failed with error $SUITE_RESULT. Execution will be terminated."
        kill -9 `jobs -p`
        exit 1
    fi
}

# Execute the unit tests, dbtest and core first in order to uncover early problems, before even
# scheduling any of the longer running JS tests
execute_test_suite "WT unittests" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=unittests"
execute_test_suite "WT dbtest" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=dbtest"
execute_test_suite "WT core" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=core"
execute_test_suite "WT core_txns" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=core_txns"

execute_test_suite "WT auth" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=auth"
execute_test_suite "WT aggregation" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=aggregation"
execute_test_suite "WT replica_sets" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=replica_sets"
execute_test_suite "WT sharding_jscore_passthrough" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=sharding_jscore_passthrough"
execute_test_suite "WT sharding" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=sharding"
