#!/bin/bash

# Strip Maven test boilerplate - show compile errors and test results only
# Usage: ./mvn-test-no-boilerplate.sh [maven test arguments]
# 
# Examples:
#   ./mvn-test-no-boilerplate.sh -Dtest=RefactorTests
#   ./mvn-test-no-boilerplate.sh -Dtest=RefactorTests#testList -Djava.util.logging.ConsoleHandler.level=INFO
#   ./mvn-test-no-boilerplate.sh -Dtest=RefactorTests#testList -Djava.util.logging.ConsoleHandler.level=FINER

mvnd test -P relaxed "$@" 2>&1 | awk '
BEGIN { 
    scanning_started = 0
    compilation_section = 0
    test_section = 0
}

# Skip all WARNING lines before project scanning starts
/INFO.*Scanning for projects/ { 
    scanning_started = 1 
    print
    next
}

# Before scanning starts, skip WARNING lines
!scanning_started && /^WARNING:/ { next }

# Show compilation errors
/COMPILATION ERROR/ { compilation_section = 1 }
/BUILD FAILURE/ && compilation_section { compilation_section = 0 }

# Show test section
/INFO.*T E S T S/ { 
    test_section = 1
    print "-------------------------------------------------------"
    print " T E S T S"
    print "-------------------------------------------------------"
    next
}

# In compilation error section, show everything
compilation_section { print }

# In test section, show everything - let user control logging with -D arguments
test_section {
    print
}

# Before test section starts, show important lines only
!test_section && scanning_started {
    if (/INFO.*Scanning|INFO.*Building|INFO.*resources|INFO.*compiler|INFO.*surefire|ERROR|FAILURE/) {
        print
    }
    # Show compilation warnings/errors
    if (/WARNING.*COMPILATION|ERROR.*/) {
        print
    }
}
'