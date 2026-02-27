#!/usr/bin/env bash
#
# bedboss pipeline dependencies installation check
#
echo -e "-----------------------------------------------------------"
echo -e "                                                           "
echo -e "             bedboss installation check                    "
echo -e "                                                           "
echo -e "-----------------------------------------------------------"

##############################################################
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

fail() {
    printf "${RED}\u2716 $*${NC}\n"
}

success() {
    printf "${GREEN}\xE2\x9C\x94 $*${NC}\n"
}

warn() {
    printf "${YELLOW}\u26A0 $*${NC}\n"
}


# Helpful functions
trim() {
    local var="$*"
    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"
    printf '%s' "$var"
}

is_executable() {
    if [ -x "$(command -v $1)" ]; then
        echo $(success "$1 is installed correctly")
        return 0
    else
        echo $(warn "WARNING: '$1' is not installed. To install '$1' check bedboss documentation: https://docs.bedbase.org/")
        return 1
    fi
}

pip_check() {
    if pip show -q $1; then
        echo $(success "package $(pip freeze | grep $1)")
        return 0
    else
        echo $(fail "package $1 is not installed")
        return 1
    fi
}

################################################################################
echo -e "Checking native installation...                            "
INSTALL_ERROR=0
INSTALL_WARNINGS=0

echo -e "Language compilers...                            "
echo -e "-----------------------------------------------------------"

# Check Python installation
if ! is_executable "python"; then
    if ! is_executable "python3"; then
      INSTALL_ERROR=$((INSTALL_ERROR+1))
    fi
fi
echo -e "-----------------------------------------------------------"
echo -e "Checking bedmaker dependencies...                            "
echo -e "-----------------------------------------------------------"

if ! pip_check "bedboss"; then
    INSTALL_ERROR=$((INSTALL_ERROR+1))
fi
if ! pip_check "refgenconf"; then
    INSTALL_ERROR=$((INSTALL_ERROR+1))
fi

if ! is_executable "bigBedToBed"; then
    INSTALL_WARNINGS=$((INSTALL_WARNINGS+1))
fi

if ! is_executable "bigWigToBedGraph"; then
    INSTALL_WARNINGS=$((INSTALL_WARNINGS+1))
fi

if ! is_executable "wigToBigWig"; then
    INSTALL_WARNINGS=$((INSTALL_WARNINGS+1))
fi

echo "Number of WARNINGS: $INSTALL_WARNINGS"

exit $INSTALL_ERROR
