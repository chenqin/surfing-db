#!/bin/bash
# Build Java modules in correct dependency order

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "======================================"
echo "Building Surfing DB Java Modules"
echo "======================================"
echo ""

# Option parsing
BUILD_ALL=true
SKIP_TESTS=""
MODULES=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --module|-m)
            BUILD_ALL=false
            MODULES+=("$2")
            shift 2
            ;;
        --skip-tests)
            SKIP_TESTS="-DskipTests"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --module,-m MODULE    Build specific module (can be specified multiple times)"
            echo "                        Modules: thrift, parquet, drsquirrel"
            echo "  --skip-tests          Skip running tests"
            echo "  --help,-h             Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                              # Build all modules"
            echo "  $0 --module parquet             # Build only Parquet module"
            echo "  $0 --module thrift --module parquet  # Build specific modules"
            echo "  $0 --skip-tests                 # Build all, skip tests"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

cd "$ROOT_DIR"

if [ "$BUILD_ALL" = true ]; then
    echo "Building all modules with parent aggregator..."
    mvn clean install $SKIP_TESTS
    echo ""
    echo "✓ All modules built successfully!"
else
    # Build specific modules in dependency order
    for module in "${MODULES[@]}"; do
        case $module in
            thrift|surfingthriftjni)
                echo "Building surfingthriftjni..."
                mvn -f surfingthriftjni/pom.xml clean package $SKIP_TESTS
                echo "✓ surfingthriftjni built"
                echo ""
                ;;
            parquet|surfing-parquet-java)
                echo "Building surfing-parquet-java..."
                mvn -f surfing-parquet-java/pom.xml clean package $SKIP_TESTS
                echo "✓ surfing-parquet-java built"
                echo ""
                ;;
            drsquirrel|drsquirrel-java-project)
                echo "Building drsquirrel-java-project..."
                mvn -f drsquirrel-java-project/pom.xml clean package $SKIP_TESTS
                echo "✓ drsquirrel-java-project built"
                echo ""
                ;;
            *)
                echo "Unknown module: $module"
                echo "Valid modules: thrift, parquet, drsquirrel"
                exit 1
                ;;
        esac
    done
fi

echo "======================================"
echo "Build Complete!"
echo "======================================"
echo ""
echo "Artifacts:"
find "$ROOT_DIR" -path "*/target/*SNAPSHOT*.jar" ! -path "*/original-*" ! -path "*/test-*" -type f 2>/dev/null | while read jar; do
    size=$(du -h "$jar" | cut -f1)
    echo "  $size  $(basename "$jar")"
done
