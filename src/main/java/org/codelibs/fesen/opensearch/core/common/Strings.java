/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.codelibs.fesen.opensearch.core.common;

import org.apache.lucene.util.BytesRefBuilder;
import org.codelibs.fesen.opensearch.ExceptionsHelper;
import org.codelibs.fesen.opensearch.OpenSearchException;
import org.codelibs.fesen.opensearch.common.Nullable;
import org.codelibs.fesen.opensearch.core.common.bytes.BytesReference;
import org.codelibs.fesen.opensearch.core.common.util.CollectionUtils;
import org.codelibs.fesen.opensearch.core.xcontent.MediaType;
import org.codelibs.fesen.opensearch.core.xcontent.ToXContent;
import org.codelibs.fesen.opensearch.core.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableSet;
import static org.codelibs.fesen.opensearch.common.util.set.Sets.newHashSet;

/**
 * String utility class.
 * <p>
 * TODO replace Strings in :server
 *
 * @opensearch.internal
 */
public class Strings {
    /**
     * The UNKNOWN_UUID_VALUE constant.
     */
    public static final String UNKNOWN_UUID_VALUE = "_na_";
    /**
     * The EMPTY_ARRAY constant.
     */
    public static final String[] EMPTY_ARRAY = new String[0];
    /**
     * The INVALID_FILENAME_CHARS constant.
     */
    public static final Set<Character> INVALID_FILENAME_CHARS = unmodifiableSet(
        newHashSet('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',')
    );

    // no instance:
    private Strings() {}

    // ---------------------------------------------------------------------
    // General convenience methods for working with Strings
    // ---------------------------------------------------------------------

    /**
     * Check that the given CharSequence is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a CharSequence that purely consists of whitespace.
     * <pre>
     * StringUtils.hasLength(null) = false
     * StringUtils.hasLength("") = false
     * StringUtils.hasLength(" ") = true
     * StringUtils.hasLength("Hello") = true
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is not null and has length
     * @see #hasText(String)
     */
    public static boolean hasLength(CharSequence str) {
        return (str != null && str.length() > 0);
    }

    /**
     * Check that the given String is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a String that purely consists of whitespace.
     *
     * @param str the String to check (may be <code>null</code>)
     * @return <code>true</code> if the String is not null and has length
     * @see Strings#hasLength(CharSequence)
     */
    public static boolean hasLength(final String str) {
        return hasLength((CharSequence) str);
    }

    /**
     * Check that the given CharSequence is either <code>null</code> or of length 0.
     * Note: Will return <code>false</code> for a CharSequence that purely consists of whitespace.
     * <pre>
     * StringUtils.isEmpty(null) = true
     * StringUtils.isEmpty("") = true
     * StringUtils.isEmpty(" ") = false
     * StringUtils.isEmpty("Hello") = false
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is either null or has a zero length
     */
    public static boolean isEmpty(final CharSequence str) {
        return hasLength(str) == false;
    }

    /**
     * Check whether the given CharSequence has actual text.
     * More specifically, returns <code>true</code> if the string not <code>null</code>,
     * its length is greater than 0, and it contains at least one non-whitespace character.
     * <pre>
     * StringUtils.hasText(null) = false
     * StringUtils.hasText("") = false
     * StringUtils.hasText(" ") = false
     * StringUtils.hasText("12345") = true
     * StringUtils.hasText(" 12345 ") = true
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is not <code>null</code>,
     *         its length is greater than 0, and it does not contain whitespace only
     * @see Character#isWhitespace
     */
    public static boolean hasText(CharSequence str) {
        if (!hasLength(str)) {
            return false;
        }
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the given String has actual text.
     * More specifically, returns <code>true</code> if the string not <code>null</code>,
     * its length is greater than 0, and it contains at least one non-whitespace character.
     *
     * @param str the String to check (may be <code>null</code>)
     * @return <code>true</code> if the String is not <code>null</code>, its length is
     *         greater than 0, and it does not contain whitespace only
     * @see Strings#hasText(CharSequence)
     */
    public static boolean hasText(String str) {
        return hasText((CharSequence) str);
    }

    /**
     * Test whether the given string matches the given substring
     * at the given index.
     *
     * @param str       the original string (or StringBuilder)
     * @param index     the index in the original string to start matching against
     * @param substring the substring to match at the given index
     * @return the substring match
     */
    public static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        for (int j = 0; j < substring.length(); j++) {
            int i = index + j;
            if (i >= str.length() || str.charAt(i) != substring.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Delete any character in a given String.
     *
     * @param inString      the original String
     * @param charsToDelete a set of characters to delete.
     *                      E.g. "az\n" will delete 'a's, 'z's and new lines.
     * @return the resulting String
     */
    public static String deleteAny(String inString, String charsToDelete) {
        if (hasLength(inString) == false || hasLength(charsToDelete) == false) {
            return inString;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inString.length(); i++) {
            char c = inString.charAt(i);
            if (charsToDelete.indexOf(c) == -1) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------
    // Convenience methods for working with formatted Strings
    // ---------------------------------------------------------------------

    /**
     * Copy the given Collection into a String array.
     * The Collection must contain String elements only.
     *
     * @param collection the Collection to copy
     * @return the String array (<code>null</code> if the passed-in
     *         Collection was <code>null</code>)
     */
    public static String[] toStringArray(final Collection<String> collection) {
        if (collection == null) {
            return null;
        }
        return collection.toArray(new String[0]);
    }

    /**
     * Split the specified string by commas to an array.
     *
     * @param s the string to split
     * @return the array of split values
     * @see String#split(String)
     */
    public static String[] splitStringByCommaToArray(final String s) {
        if (s == null || s.isEmpty()) return Strings.EMPTY_ARRAY;
        else return s.split(",");
    }

    /**
     * Split a String at the first occurrence of the delimiter.
     * Does not include the delimiter in the result.
     *
     * @param toSplit   the string to split
     * @param delimiter to split the string up with
     * @return a two element array with index 0 being before the delimiter, and
     *         index 1 being after the delimiter (neither element includes the delimiter);
     *         or <code>null</code> if the delimiter wasn't found in the given input String
     */
    public static String[] split(String toSplit, String delimiter) {
        if (hasLength(toSplit) == false || hasLength(delimiter) == false) {
            return null;
        }
        int offset = toSplit.indexOf(delimiter);
        if (offset < 0) {
            return null;
        }
        String beforeDelimiter = toSplit.substring(0, offset);
        String afterDelimiter = toSplit.substring(offset + delimiter.length());
        return new String[] { beforeDelimiter, afterDelimiter };
    }

    /**
     * Tokenize the given String into a String array via a StringTokenizer.
     * Trims tokens and omits empty tokens.
     * <p>The given delimiters string is supposed to consist of any number of
     * delimiter characters. Each of those characters can be used to separate
     * tokens. A delimiter is always a single character; for multi-character
     * delimiters, consider using <code>delimitedListToStringArray</code>
     *
     * @param s        the String to tokenize
     * @param delimiters the delimiter characters, assembled as String
     *                   (each of those characters is individually considered as delimiter).
     * @return an array of the tokens
     * @see StringTokenizer
     * @see String#trim()
     * @see Strings#delimitedListToStringArray
     */
    public static String[] tokenizeToStringArray(final String s, final String delimiters) {
        if (s == null) {
            return Strings.EMPTY_ARRAY;
        }
        return toStringArray(tokenizeToCollection(s, delimiters, ArrayList::new));
    }

    /**
     * Tokenizes the specified string to a collection using the specified delimiters as the token delimiters. This method trims whitespace
     * from tokens and ignores empty tokens.
     *
     * @param s          the string to tokenize.
     * @param delimiters the token delimiters
     * @param supplier   a collection supplier
     * @param <T>        the type of the collection
     * @return the tokens
     * @see StringTokenizer
     */
    private static <T extends Collection<String>> T tokenizeToCollection(
        final String s,
        final String delimiters,
        final Supplier<T> supplier
    ) {
        if (s == null) {
            return null;
        }
        final StringTokenizer tokenizer = new StringTokenizer(s, delimiters);
        final T tokens = supplier.get();
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken().trim();
            if (token.length() > 0) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    /**
     * Take a String which is a delimited list and convert it to a String array.
     * <p>A single delimiter can consists of more than one character: It will still
     * be considered as single delimiter string, rather than as bunch of potential
     * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
     *
     * @param str           the input String
     * @param delimiter     the delimiter between elements (this is a single delimiter,
     *                      rather than a bunch individual delimiter characters)
     * @param charsToDelete a set of characters to delete. Useful for deleting unwanted
     *                      line breaks: e.g. "\r\n\f" will delete all new lines and line feeds in a String.
     * @return an array of the tokens in the list
     * @see #tokenizeToStringArray
     */
    public static String[] delimitedListToStringArray(String str, String delimiter, String charsToDelete) {
        if (str == null) {
            return Strings.EMPTY_ARRAY;
        }
        if (delimiter == null) {
            return new String[] { str };
        }
        List<String> result = new ArrayList<>();
        if ("".equals(delimiter)) {
            for (int i = 0; i < str.length(); i++) {
                result.add(deleteAny(str.substring(i, i + 1), charsToDelete));
            }
        } else {
            int pos = 0;
            int delPos;
            while ((delPos = str.indexOf(delimiter, pos)) != -1) {
                result.add(deleteAny(str.substring(pos, delPos), charsToDelete));
                pos = delPos + delimiter.length();
            }
            if (str.length() > 0 && pos <= str.length()) {
                // Add rest of String, but not in case of empty input.
                result.add(deleteAny(str.substring(pos), charsToDelete));
            }
        }
        return toStringArray(result);
    }

    /**
     * Take a String which is a delimited list and convert it to a String array.
     * <p>A single delimiter can consists of more than one character: It will still
     * be considered as single delimiter string, rather than as bunch of potential
     * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
     *
     * @param str       the input String
     * @param delimiter the delimiter between elements (this is a single delimiter,
     *                  rather than a bunch individual delimiter characters)
     * @return an array of the tokens in the list
     * @see Strings#tokenizeToStringArray
     */
    public static String[] delimitedListToStringArray(String str, String delimiter) {
        return delimitedListToStringArray(str, delimiter, null);
    }

    /**
     * Convert a CSV list into an array of Strings.
     *
     * @param str the input String
     * @return an array of Strings, or the empty array in case of empty input
     */
    public static String[] commaDelimitedListToStringArray(String str) {
        return delimitedListToStringArray(str, ",");
    }

    /**
     * Convenience method to convert a CSV string list to a set.
     * Note that this will suppress duplicates.
     *
     * @param str the input String
     * @return a Set of String entries in the list
     */
    public static Set<String> commaDelimitedListToSet(String str) {
        Set<String> set = new TreeSet<>();
        String[] tokens = commaDelimitedListToStringArray(str);
        set.addAll(Arrays.asList(tokens));
        return set;
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll   the Collection to display
     * @param delim  the delimiter to use (probably a ",")
     * @param prefix the String to start each element with
     * @param suffix the String to end each element with
     * @return the delimited String
     */
    public static String collectionToDelimitedString(Iterable<?> coll, String delim, String prefix, String suffix) {
        StringBuilder sb = new StringBuilder();
        collectionToDelimitedString(coll, delim, prefix, suffix, sb);
        return sb.toString();
    }

    /**
     * Performs the collection to delimited string step.
     *
     * @param coll the coll
     * @param delim the delim
     * @param prefix the prefix
     * @param suffix the suffix
     * @param sb the sb
     */
    public static void collectionToDelimitedString(Iterable<?> coll, String delim, String prefix, String suffix, StringBuilder sb) {
        Iterator<?> it = coll.iterator();
        while (it.hasNext()) {
            sb.append(prefix).append(it.next()).append(suffix);
            if (it.hasNext()) {
                sb.append(delim);
            }
        }
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll  the Collection to display
     * @param delim the delimiter to use (probably a ",")
     * @return the delimited String
     */
    public static String collectionToDelimitedString(Iterable<?> coll, String delim) {
        return collectionToDelimitedString(coll, delim, "", "");
    }

    /**
     * Convenience method to return a Collection as a CSV String.
     * E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll the Collection to display
     * @return the delimited String
     */
    public static String collectionToCommaDelimitedString(Iterable<?> coll) {
        return collectionToDelimitedString(coll, ",");
    }

    /**
     * Convenience method to return a String array as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param arr   the array to display
     * @param delim the delimiter to use (probably a ",")
     * @return the delimited String
     */
    public static String arrayToDelimitedString(Object[] arr, String delim) {
        StringBuilder sb = new StringBuilder();
        arrayToDelimitedString(arr, delim, sb);
        return sb.toString();
    }

    /**
     * Performs the array to delimited string step.
     *
     * @param arr the arr
     * @param delim the delim
     * @param sb the sb
     */
    public static void arrayToDelimitedString(Object[] arr, String delim, StringBuilder sb) {
        if (isEmpty(arr)) {
            return;
        }
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(arr[i]);
        }
    }

    /**
     * Format the double value with a single decimal points, trimming trailing '.0'.
     *
     * @param value the value
     * @param suffix the suffix
     * @return the format1 decimals
     */
    public static String format1Decimals(double value, String suffix) {
        String p = String.valueOf(value);
        int ix = p.indexOf('.') + 1;
        int ex = p.indexOf('E');
        char fraction = p.charAt(ix);
        if (fraction == '0') {
            if (ex != -1) {
                return p.substring(0, ix - 1) + p.substring(ex) + suffix;
            } else {
                return p.substring(0, ix - 1) + suffix;
            }
        } else {
            if (ex != -1) {
                return p.substring(0, ix) + fraction + p.substring(ex) + suffix;
            } else {
                return p.substring(0, ix) + fraction + suffix;
            }
        }
    }

    /**
     * Determine whether the given array is empty:
     * i.e. <code>null</code> or of zero length.
     *
     * @param array the array to check
     */
    private static boolean isEmpty(final Object[] array) {
        return (array == null || array.length == 0);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. The content is not pretty-printed
     * nor human readable.
     *
     * @param mediaType the media type
     * @param toXContent the to XContent
     * @return a string representation of this instance
     */
    public static String toString(MediaType mediaType, ToXContent toXContent) {
        return toString(mediaType, toXContent, false, false);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. Allows to control whether the outputted
     * json needs to be pretty printed and human readable.
     *
     * @param mediaType the media type
     *
     * @param toXContent the to XContent
     * @param pretty the pretty
     * @param human the human
     * @return a string representation of this instance
     */
    public static String toString(MediaType mediaType, ToXContent toXContent, boolean pretty, boolean human) {
        return toString(mediaType, toXContent, ToXContent.EMPTY_PARAMS, pretty, human);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * Allows to control whether the outputted json needs to be pretty printed and human readable.
     */
    private static String toString(MediaType mediaType, ToXContent toXContent, ToXContent.Params params, boolean pretty, boolean human) {
        try {
            XContentBuilder builder = createBuilder(mediaType, pretty, human);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return builder.toString();
        } catch (IOException e) {
            try {
                XContentBuilder builder = createBuilder(mediaType, pretty, human);
                builder.startObject();
                builder.field("error", "error building toString out of XContent: " + e.getMessage());
                builder.field("stack_trace", ExceptionsHelper.stackTrace(e));
                builder.endObject();
                return builder.toString();
            } catch (IOException e2) {
                throw new OpenSearchException("cannot generate error message for deserialization", e);
            }
        }
    }

    private static XContentBuilder createBuilder(MediaType mediaType, boolean pretty, boolean human) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(mediaType.xContent());
        if (pretty) {
            builder = builder.prettyPrint();
        }
        if (human) {
            builder = builder.humanReadable(true);
        }
        return builder;
    }

    /**
     * Returns the null or empty flag.
     *
     * @param s the s
     * @return the null or empty flag
     */
    public static boolean isNullOrEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Returns this instance as lowercase ASCII.
     *
     * @param in the input to read from
     * @return the lowercase ASCII
     */
    public static String toLowercaseAscii(String in) {
        StringBuilder out = new StringBuilder();
        Iterator<Integer> iter = in.codePoints().iterator();
        while (iter.hasNext()) {
            int codepoint = iter.next();
            if (codepoint > 128) {
                out.appendCodePoint(codepoint);
            } else {
                out.appendCodePoint(Character.toLowerCase(codepoint));
            }
        }
        return out.toString();
    }

}
