#!/bin/bash
cd /Users/sixten/Documents/MYRUST/tableclass

./target/release/binrecepie ./src/bins/binrecepie/data popingredients >./src/bins/binrecepie/data/svgs/popingredients.svg 
./target/release/binrecepie ./src/bins/binrecepie/data complexrecepies >./src/bins/binrecepie/data/svgs/complexrecepies.svg 

