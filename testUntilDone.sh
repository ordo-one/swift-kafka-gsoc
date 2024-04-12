#!/bin/bash


my_command () { swift test --filter "testProduceAndConsumeWithTransaction"; return $?; }

until ! my_command; do
    echo "success"
    # potentially, other code follows...
done

