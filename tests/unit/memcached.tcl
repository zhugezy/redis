proc exec_command {text} {
	r write $text
	r flush
	set res [r read]
	return $res
}

start_server {tags {"memcached"} overrides {protocol memcache}} {
	test {ERROR test} {
		exec_command "flush_all\r\n"

		set res [exec_command "ge a\r\n"]
		assert_equal $res "ERROR"
	}

	test {get/set basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "set a 1 3\r\n"]
		assert_equal $res "CLIENT_ERROR bad command line format"

		set res [exec_command "set a 1 0 3\r\nvaa\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "get a\r\n"]
		assert_equal $res "VALUE a 1 3\r\nvaa\r\nEND"

		set res [exec_command "set a 2 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "get a\r\n"]
		assert_equal $res "VALUE a 2 4\r\nvala\r\nEND"

		set res [exec_command "get b\r\n"]
		assert_equal $res "END"

		set res [exec_command "set b 998244353 0 4\r\nvalb\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "get a d b\r\n"]
		assert_equal $res "VALUE a 2 4\r\nvala\r\nVALUE b 998244353 4\r\nvalb\r\nEND"
	}

	test {gets basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "set a 1 0 3\r\nvaa\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 1 3 1\r\nvaa\r\nEND"

		set res [exec_command "set a 2 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 2 4 2\r\nvala\r\nEND"

		set res [exec_command "set b 998244353 0 4\r\nvalb\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "gets a d b\r\n"]
		assert_equal $res "VALUE a 2 4 2\r\nvala\r\nVALUE b 998244353 4 1\r\nvalb\r\nEND"
	}

	test {set expiration} {
		exec_command "flush_all\r\n"

		set res [exec_command "set a 1 1 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		after 500

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 1 4 1\r\nvala\r\nEND"

		after 510

		set res [exec_command "gets a\r\n"]
		assert_equal $res "END"
	}

	test {flush_all} {
		set res [exec_command "set a 1 2 4\r\nvala\r\n"]

		exec_command "flush_all\r\n"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "END"
	}

	test {add/replace basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "add a 1 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "add a 1 0 4\r\nvara\r\n"]
		assert_equal $res "NOT_STORED"

		set res [exec_command "replace b 1 0 4\r\nvarb\r\n"]
		assert_equal $res "NOT_STORED"

		set res [exec_command "replace a 3 0 4\r\narav\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "gets a b\r\n"]
		assert_equal $res "VALUE a 3 4 2\r\narav\r\nEND"
	}

	test {add/replace expiration} {
		exec_command "flush_all\r\n"

		set res [exec_command "add a 1 1 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		after 250

		set res [exec_command "add a 1 2 4\r\nva2a\r\n"]
		assert_equal $res "NOT_STORED"

		after 250

		set res [exec_command "replace a 1 2 4\r\nva3a\r\n"]
		assert_equal $res "STORED"

		after 1000

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 1 4 2\r\nva3a\r\nEND"

		after 1100

		set res [exec_command "gets a\r\n"]
		assert_equal $res "END"

		set res [exec_command "add a 1 1 4\r\nva4a\r\n"]
		assert_equal $res "STORED"

		after 1100

		set res [exec_command "gets a\r\n"]
		assert_equal $res "END"
	}

	test {auth basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "auth aaa bbb\r\n"]
		assert_equal $res "OK"

		set res [exec_command "auth ccc\r\n"]
		assert_equal $res "CLIENT_ERROR bad command line format"
	}

	test {cas basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "cas a 1 0 4 2\r\nvala\r\n"]
		assert_equal $res "NOT_FOUND"

		set res [exec_command "set a 1 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "cas a 1 0 4 0\r\nva2a\r\n"]
		assert_equal $res "EXISTS"

		set res [exec_command "cas a 1 0 4 2\r\nva3a\r\n"]
		assert_equal $res "EXISTS"
		
		set res [exec_command "cas a 1 0 4 1\r\nva4a\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "cas a 1 0 4\r\n"]
		assert_equal $res "CLIENT_ERROR bad command line format"
		
	}

	test {prepend/append basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "append a 1 0 2\r\na1\r\n"]
		assert_equal $res "NOT_STORED"

		set res [exec_command "set a 1 0 2\r\na1\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "append a 2 0 2\r\na2\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 1 4 2\r\na1a2\r\nEND"

		set res [exec_command "prepend a 3 0 2\r\na3\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 1 6 3\r\na3a1a2\r\nEND"
	}

	test {delete basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "set a 1 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "set b 1 0 4\r\nvalb\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "delete a\r\n"]
		assert_equal $res "DELETED"

		set res [exec_command "delete a\r\n"]
		assert_equal $res "NOT_FOUND"

		set res [exec_command "gets b\r\n"]
		assert_equal $res "VALUE b 1 4 1\r\nvalb\r\nEND"
	}

	test {incr basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "set a 1 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "incr a 1\r\n"]
		assert_equal $res "CLIENT_ERROR cannot increment or decrement non-numeric value"

		exec_command "flush_all\r\n"

		set res [exec_command "incr a 1\r\n"]
		assert_equal $res "NOT_FOUND"

		set res [exec_command "set a 2147 0 3\r\n990\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "incr a 11\r\n"]
		assert_equal $res "1001"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 2147 4 2\r\n1001\r\nEND"

		set res [exec_command "incr a -1\r\n"]
		assert_equal $res "CLIENT_ERROR invalid numeric delta argument"
	}

	test {incr overflow} {
		exec_command "flush_all\r\n"
		#uint64_t max is 18446744073709551615.
		set res [exec_command "set a 0 0 20\r\n18446744073709551614\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "incr a 1\r\n"]
		assert_equal $res "18446744073709551615"

		set res [exec_command "incr a 2\r\n"]
		assert_equal $res "1"

		set res [exec_command "set a 0 0 20\r\n18446744073709551617\r\n"]
		assert_equal $res "STORED" 

		set res [exec_command "incr a 1\r\n"]
		assert_equal $res "CLIENT_ERROR cannot increment or decrement non-numeric value"
	}

	test {decr basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "set a 1 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "decr a 1\r\n"]
		assert_equal $res "CLIENT_ERROR cannot increment or decrement non-numeric value"

		exec_command "flush_all\r\n"

		set res [exec_command "decr a 1\r\n"]
		assert_equal $res "NOT_FOUND"

		set res [exec_command "set a 2147 0 4\r\n1001\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "decr a 11\r\n"]
		assert_equal $res "990"

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 2147 3 2\r\n990\r\nEND"

		set res [exec_command "decr a -1\r\n"]
		assert_equal $res "CLIENT_ERROR invalid numeric delta argument"
	}

	test {decr overflow} {
		exec_command "flush_all\r\n"
		#uint64_t max is 18446744073709551615.
		set res [exec_command "set a 0 0 1\r\n2\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "decr a 1\r\n"]
		assert_equal $res "1"

		set res [exec_command "decr a 2\r\n"]
		assert_equal $res "0"

		set res [exec_command "set a 0 0 20\r\n18446744073709551617\r\n"]
		assert_equal $res "STORED" 

		set res [exec_command "decr a 1\r\n"]
		assert_equal $res "CLIENT_ERROR cannot increment or decrement non-numeric value"
	}

	test {version(command) basic} {
		set res [exec_command "version\r\n"]
		assert_match {VERSION*} $res 
	}

	test {touch basic} {
		exec_command "flush_all\r\n"

		set res [exec_command "touch a\r\n"]
		assert_equal $res "CLIENT_ERROR bad command line format"

		set res [exec_command "touch a 1\r\n"]
		assert_equal $res "NOT_FOUND"

		set res [exec_command "set a 1 1 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		after 500

		set res [exec_command "touch a 2\r\n"]
		assert_equal $res "TOUCHED"

		after 1500

		set res [exec_command "gets a\r\n"]
		assert_equal $res "VALUE a 1 4 1\r\nvala\r\nEND"

		after 600

		set res [exec_command "gets a\r\n"]
		assert_equal $res "END"
	}

	test {get/set binary basic} {
		exec_command "flush_all\r\n"
		#TODO: change into binary
		set res [exec_command "set a 1 0 3\r\nvaa\r\n"]
		assert_equal $res "STORED"

		set res [exec_command "set a 2 0 4\r\nvala\r\n"]
		assert_equal $res "STORED"

		set res [exec_command [binary format H* 80000001000000000000000100000000000000000000000061]]
		assert_equal $res [binary format H* 8100000004000000000000080000000000000000000000020000000276616C61]
	} 
}