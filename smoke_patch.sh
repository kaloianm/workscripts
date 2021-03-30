#! /bin/bash
#
# This is a script for smoke-testing a patch before submitting it for an Evergreen run. Allows the
# patch to compile and execute faster and to weed out simple programming errors without wasting AWS
# time and money.
#
# Export a patch of the changes though `git format-patch` into a file:
#  > git format-patch --stdout <Git hash> > Patch.patch
#  > scp Patch.patch BUILD-MACHINE:~/
#
# Smoke the patch using the following command line:
#  > smoke_patch.sh ~/Patch.patch BRANCH-NAME BUILD-TYPE [<BUILD-ROOT-DIR>]
#

###################################################################################################
# Build and test execution constants
###################################################################################################
export CPUS_FOR_ICECC_BUILD=32
export CPUS_FOR_LOCAL_BUILD=12
export CPUS_FOR_TESTS=6

###################################################################################################
# Command line parameters parsing
###################################################################################################
if [ "$#" -lt 3 ]; then
    echo "Error: illegal number of parameters $#"
    echo "Usage: smoke_patch.sh <Patch-File> <v4.0|v4.2|v4.4|master> <Build Type> [<Build root>]"
    exit 1
fi

# Process the Patch-File argument
export PATCHFILE=$1
echo "Using patch file $PATCHFILE"
if [ ! -f $PATCHFILE ]; then
    echo "Error: patch file $PATCHFILE not found"
    exit 1
fi

# Process the Branch-Name argument
export BRANCH=$2
echo "Running against branch $BRANCH"
if [ "$BRANCH" == "master" ] || [ "$BRANCH" == "v4.4" ]; then
    export PATH=/opt/mongodbtoolchain/v3/bin:$PATH
    export ICECREAM_FLAGS="CCACHE=ccache ICECC=icecc"
    export GENERATE_NINJA_FLAGS="--ninja"
    export NINJA_TARGET="install-all"
    export TOOLSDIR=/home/kaloianm/mongodb/4.4.4
    export RESMOKE_COMMAND="buildscripts/resmoke.py run"
elif [ "$BRANCH" == "v4.2" ]; then
    export PATH=/opt/mongodbtoolchain/v3/bin:$PATH
    export ICECREAM_FLAGS="--icecream"
    export NINJA_TARGET="all"
    export TOOLSDIR=/home/kaloianm/mongodb/4.2.12
    export RESMOKE_COMMAND="buildscripts/resmoke.py run"
elif [ "$BRANCH" == "v4.0" ]; then
    export PATH=/opt/mongodbtoolchain/v2/bin:$PATH
    export ICECREAM_FLAGS="--icecream"
    export NINJA_TARGET="all"
    export TOOLSDIR=/home/kaloianm/mongodb/4.0.18
    export RESMOKE_COMMAND="buildscripts/resmoke.py"
fi

export BUILD_NINJA_COMMAND="buildscripts/scons.py --ssl $GENERATE_NINJA_FLAGS MONGO_VERSION=0.0.0 MONGO_GIT_HASH=unknown VARIANT_DIR=ninja"

# Process the Build-Type argument
export BUILDTYPE=$3
echo "Performing build type $BUILDTYPE"

if [ "$BUILDTYPE" == "gcc-opt" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_gcc.vars        --dbg=off   --opt=on"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "gcc-dbg" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_gcc.vars        --dbg=on    --opt=off"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "clang-opt" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_clang.vars      --dbg=off   --opt=on"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "clang-dbg" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_clang.vars      --dbg=on    --opt=off"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "gcc" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_gcc.vars        --dbg=on    --opt=on"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "clang" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_clang.vars      --dbg=on    --opt=on"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "clang-dynamic" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_clang.vars      --dbg=on    --opt=on    --link-model=dynamic"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "gcc-dynamic" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_gcc.vars        --dbg=on    --opt=on    --link-model=dynamic"
    export CPUS_FOR_BUILD=$CPUS_FOR_ICECC_BUILD
elif [ "$BUILDTYPE" == "ubsan" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND $ICECREAM_FLAGS    --variables-files=etc/scons/mongodbtoolchain_stable_clang.vars      --dbg=on    --opt=on                            --allocator=system      --sanitize=undefined,address"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
elif [ "$BUILDTYPE" == "system-gcc-dbg" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND                    --jlink=4   CC=/usr/bin/gcc CXX=/usr/bin/g++ CCACHE=ccache          --dbg=on    --opt=off"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
elif [ "$BUILDTYPE" == "system-clang-dbg" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND                    --jlink=4   CC=/usr/bin/clang CXX=/usr/bin/clang++ CCACHE=ccache    --dbg=on    --opt=off"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
elif [ "$BUILDTYPE" == "system-gcc-opt" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND                    --jlink=4   CC=/usr/bin/gcc CXX=/usr/bin/g++ CCACHE=ccache          --dbg=off   --opt=on"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
elif [ "$BUILDTYPE" == "system-clang-opt" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND                    --jlink=4   CC=/usr/bin/clang CXX=/usr/bin/clang++ CCACHE=ccache    --dbg=off   --opt=on"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
elif [ "$BUILDTYPE" == "system-clang-dynamic" ]; then
    export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND                    --jlink=4   CC=/usr/bin/clang CXX=/usr/bin/clang++ CCACHE=ccache    --dbg=on    --opt=on    --link-model=dynamic"
    export CPUS_FOR_BUILD=$CPUS_FOR_LOCAL_BUILD
else
    echo "Error: invalid build type ($BUILDTYPE) specified"
    exit 1
fi

# Process the Build-Root-Directory argument
export BUILDROOTDIR="/tmp/$USER"
if [ ! -z $4 ]; then
    BUILDROOTDIR=$4
fi

###################################################################################################
# Environment construction (cloning repositories, etc.)
###################################################################################################
export TESTRUNDIR="$BUILDROOTDIR/smoke_patch"
echo "Using test run directory $TESTRUNDIR"
if [ -d $TESTRUNDIR ]; then
    echo "Deleting previous test run directory $TESTRUNDIR ..."
    rm -rf $TESTRUNDIR
    if [ $? -ne 0 ]; then
        echo "Error: unable to delete previous test run directory $?"
        exit 1
    fi
fi

mkdir --parents "$TESTRUNDIR"
if [ $? -ne 0 ]; then
    echo "Error: unable to create test run directory $?"
    exit 1
fi

export TESTDBPATHDIR="$TESTRUNDIR/db"
mkdir "$TESTDBPATHDIR"

echo "Cloning the mongo repository ..."
git clone -b $BRANCH --depth 1 git@github.com:mongodb/mongo.git "$TESTRUNDIR/mongo"

echo "Cloning the mongo-enterprise-modules repository ..."
git clone -b $BRANCH --depth 1 git@github.com:10gen/mongo-enterprise-modules.git "$TESTRUNDIR/mongo/src/mongo/db/modules/enterprise"

if [ "$BRANCH" == "master" ] || [ "$BRANCH" == "v4.4" ]; then
    echo "Using native ninja support ..."
else
    echo "Cloning the ninja repository ..."
    git clone --depth 1 git@github.com:RedBeard0531/mongo_module_ninja.git "$TESTRUNDIR/mongo/src/mongo/db/modules/ninja"
fi

###################################################################################################
# Building and linting sources
###################################################################################################
pushd "$TESTRUNDIR/mongo"

echo "Applying patch file $PATCHFILE"
git am $PATCHFILE
if [ $? -ne 0 ]; then
    echo "Error: git apply failed with error $?"
    exit 1
fi

export FLAGS_FOR_TEST="--log=file --dbpathPrefix=$TESTDBPATHDIR --shuffle --continueOnFailure --basePort=12000"

# Construct the scons, ninja and linter command lines
export LINT_CMDLINE="$BUILD_NINJA_COMMAND -j $CPUS_FOR_LOCAL_BUILD --no-cache --build-dir=$TESTRUNDIR/mongo/lint lint"
export BUILD_NINJA_COMMAND="$BUILD_NINJA_COMMAND build.ninja"
export BUILD_CMDLINE="ninja -j $CPUS_FOR_BUILD $NINJA_TARGET"

# Start the slower builder and linter first so that the slower tasks can overlap with it
echo "Starting lint and build ..."

echo "Lint command line: $LINT_CMDLINE" > lint.log
time $LINT_CMDLINE >> lint.log 2>&1 &
PID_lint=$!

echo "Build command line: $BUILD_NINJA_COMMAND" > build.log
time $BUILD_NINJA_COMMAND >> build.log 2>&1 &
PID_build_ninja=$!

# Copy any binaries which are needed for running tests
echo "Copying executables to support tests ..."
cp "$TOOLSDIR/bsondump" `pwd`
cp "$TOOLSDIR/mongodump" `pwd`
cp "$TOOLSDIR/mongorestore" `pwd`

echo "Waiting for build ninja process $PID_build_ninja to complete ..."
wait $PID_build_ninja
if [ $? -ne 0 ]; then
    echo "Error: build ninja failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

echo "Starting build ..."
echo $BUILD_CMDLINE >> build.log
time $BUILD_CMDLINE >> build.log 2>&1 &
PID_build=$!

# Wait for the build and linter to complete
echo "Waiting for build process $PID_build to complete ..."
wait $PID_build
if [ $? -ne 0 ]; then
    echo "Error: build failed with error $?"
    kill -9 `jobs -p`
    exit 1
fi

###################################################################################################
# Test execution (build and lint are complete at this point)
###################################################################################################

###################################################################################################
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
                echo "Error: lint failed with error $TIMEOUT_RESULT"
                # kill -9 `jobs -p`
                # exit 1
            fi
        fi
        unset PID_lint
    fi

    # Kick off the actual test suite
    time $2; local SUITE_RESULT=$?
    if [ $SUITE_RESULT -ne 0 ]; then
        echo "Error: suite $1 failed with error $SUITE_RESULT and execution will be terminated"
        kill -9 `jobs -p`
        exit 1
    fi
}

# The GSSAPI tests require a Kerberos server set up and they can never pass, so exclude them from
# the run
perl -ni -e 'print unless /sasl_authentication_session_gssapi_test/' build/unittests.txt

# Execute the unit tests, dbtest and core first in order to uncover early problems, before even
# scheduling any of the longer running JS tests
execute_test_suite "WT unittests" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=unittests"
execute_test_suite "WT dbtest" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=dbtest"
execute_test_suite "WT core" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=core"
execute_test_suite "WT core_txns" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=core_txns"

# Slower suites
execute_test_suite "WT aggregation" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=aggregation"
execute_test_suite "WT replica_sets" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=replica_sets"
execute_test_suite "WT sharding_jscore_passthrough" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=sharding_jscore_passthrough"
execute_test_suite "WT auth" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=auth"
execute_test_suite "WT sharding" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=sharding"

# Concurrency suites
execute_test_suite "WT concurrency_sharded_with_stepdowns_and_balancer" "$RESMOKE_COMMAND -j $CPUS_FOR_TESTS $FLAGS_FOR_TEST --storageEngine=wiredTiger --suites=concurrency_sharded_with_stepdowns_and_balancer"
