mkdir libs
cd libs
curl -Ol http://www.quviq.com/wp-content/uploads/2015/09/eqcmini-2.01.0.zip
unzip eqcmini-2.01.0.zip
cd ..
ERL_LIBS=./libs
find / -name empd
empd &
./rebar3 as eqc eqc --sname eqc
