SELECT
    substringIndex('a.b.c', '.', toUInt64(18446744073709551615)),
    substringIndex(materialize('a.b.c'), '.', toUInt64(18446744073709551615)),
    substringIndex('a.b.c', '.', materialize(toUInt64(18446744073709551615))),
    substringIndex(materialize('a.b.c'), '.', materialize(toUInt64(18446744073709551615))),
    substringIndexUTF8('a。b。c', '。', toUInt64(18446744073709551615)),
    substringIndexUTF8(materialize('a。b。c'), '。', toUInt64(18446744073709551615)),
    substringIndexUTF8('a。b。c', '。', materialize(toUInt64(18446744073709551615))),
    substringIndexUTF8(materialize('a。b。c'), '。', materialize(toUInt64(18446744073709551615)));
