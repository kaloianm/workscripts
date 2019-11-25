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

export TESTRUNDIR="/mnt/SSD/$USER/Data/smoke_patch"
echo "Using test run directory $TESTRUNDIR"
if [ -d $TESTRUNDIR ]; then
    echo "Deleting previous test run directory $TESTRUNDIR ..."
    rm -rf $TESTRUNDIR
fi

mkdir "$TESTRUNDIR"

export TESTDBPATHDIR="$TESTRUNDIR/db"
mkdir "$TESTDBPATHDIR"

export TOOLSDIR=/home/kaloianm/mongodb/4.2.1

# Use all the tools from the mongodb toolchain instead of those installed on the system
export MONGODBTOOLCHAIN="/opt/mongodbtoolchain/v3/bin"
export PATH=$MONGODBTOOLCHAIN:$PATH

echo "Build environment will be using Python from `which python`"

export RESMOKECMD="python3 buildscripts/resmoke.py"
export SCONSCMD="python3 buildscripts/scons.py"

export CPUS_FOR_ICECC_BUILD=500
export CPUS_FOR_LOCAL_BUILD=12
export CPUS_FOR_LINT=12
export CPUS_FOR_TESTS=24

export MONGO_VERSION_AND_GITHASH="MONGO_VERSION=0.0.0 MONGO_GIT_HASH=unknown"

if [ "$2" == "dynamic" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl --link-model=dynamic --allocator=system CC=clang CXX=clang++ --icecream"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$2" == "clang" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl CC=clang CXX=clang++ --icecream"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$2" == "opt" ]; then
    export FLAGS_FOR_BUILD="--dbg=off --opt=on --ssl --icecream"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$2" == "dbg" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=off --ssl --icecream"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$2" == "system-clang" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl CC=/usr/bin/clang CXX=/usr/bin/clang++"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
elif [ "$2" == "ubsan" ]; then
    export FLAGS_FOR_BUILD="--dbg=on --opt=on --ssl --allocator=system --sanitize=undefined,address CC=clang CXX=clang++"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
else
    echo "Invalid build type or no build type specified"
    exit 1
fi

export FLAGS_FOR_TEST="--log=file --dbpathPrefix=$TESTDBPATHDIR --shuffle --continueOnFailure --basePort=12000"

# Construct the scons, ninja and linter command lines
export BUILD_NINJA_CMDLINE="$SCONSCMD --variables-files=etc/scons/mongodbtoolchain_v3_clang.vars $FLAGS_FOR_BUILD $MONGO_VERSION_AND_GITHASH VARIANT_DIR=ninja build.ninja"
export BUILD_CMDLINE="ninja -j $CPUS_FOR_BUILD all"
export LINT_CMDLINE="$SCONSCMD -j $CPUS_FOR_LINT $FLAGS_FOR_BUILD $MONGO_VERSION_AND_GITHASH --no-cache --build-dir=$TESTRUNDIR/mongo/lint lint"

# Clone the repositories
echo "Cloning the mongo repository ..."
git clone --depth 1 git@github.com:mongodb/mongo.git "$TESTRUNDIR/mongo"

echo "Cloning the enterprise repository ..."
git clone --depth 1 git@github.com:10gen/mongo-enterprise-modules.git "$TESTRUNDIR/mongo/src/mongo/db/modules/enterprise"

echo "Cloning the ninja repository ..."
git clone --depth 1 git@github.com:RedBeard0531/mongo_module_ninja.git "$TESTRUNDIR/mongo/src/mongo/db/modules/ninja"

# Switch to the root source directory
pushd "$TESTRUNDIR/mongo"

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
    if [ -n "$PID_lint" ]; then
        if ! kill -0 $PID_lint > /dev/null 2>&1; then
            echo "Lint process $PID_lint has completed, joining to get its error code ..."
            wait $PID_lint; local TIMEOUT_RESULT=$?
            if [ $TIMEOUT_RESULT -ne 0 ]; then
                echo "lint failed with error $TIMEOUT_RESULT"
                kill -9 `jobs -p`
                exit 1
            fi
        fi
        unset PID_lint
    fi

    # Kick off the actual test suite
    time $2; local SUITE_RESULT=$?
    if [ $SUITE_RESULT -ne 0 ]; then
        echo "Suite $1 failed with error $SUITE_RESULT. Execution will be terminated."
        kill -9 `jobs -p`
        exit 1
    fi
}


# The GSSAPI tests require a Kerberos server set up and they can never pass, so exclude them
# from the run
perl -ni -e 'print unless /sasl_authentication_session_gssapi_test/' build/unittests.txt

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

execute_test_suite "WT concurrency_sharded_with_stepdowns_and_balancer" "$RESMOKECMD -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=concurrency_sharded_with_stepdowns_and_balancer"
