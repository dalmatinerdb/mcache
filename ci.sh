mkdir libs
cd libs
curl -Ol http://www.quviq.com/wp-content/uploads/2015/09/eqcmini-2.01.0.zip
unzip eqcmini-2.01.0.zip
cd ..
epmd &
export ERL_LIBS=./libs 
./rebar3 as local eqc
./rebar3 dialyzer
