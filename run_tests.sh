#!/bin/bash

# Run pytest
pytest

# Check for autopep8 compliance
autopep8 --recursive --diff function/ src/ star_schema/src/ star_schema/src/app/ star_schema/src/utils/ star_schema/test/ test/

# If autopep8 has changes, ask the user if they want to apply them
read -p "Apply autopep8 changes in function/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place function/
fi

read -p "Apply autopep8 changes in src/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place src/
fi

read -p "Apply autopep8 changes in star_schema/src/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place star_schema/src/
fi

read -p "Apply autopep8 changes in star_schema/src/app/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place star_schema/src/app/
fi

read -p "Apply autopep8 changes in star_schema/src/utils/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place star_schema/src/utils/
fi

read -p "Apply autopep8 changes in star_schema/test/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place star_schema/test/
fi

read -p "Apply autopep8 changes in test/ ? [y/N]: " APPLY_CHANGES
if [[ "$APPLY_CHANGES" =~ ^[Yy]$ ]]; then
    autopep8 --recursive --in-place test/
fi

# first run chmod +x run_tests.sh in your terminal to allow permissions to run this file

# run this file with the following command from the root of the directory: ./run_tests.sh

