import { describe, expect, test } from 'bun:test'
import { convertXHashtagsToBiliFormat } from './bili-hashtag'

describe('convertXHashtagsToBiliFormat', () => {
    // twitter-text conformance (autolink.yml) baseline cases
    test('wraps a simple hashtag after a space', () => {
        expect(convertXHashtagsToBiliFormat('text #hashtag here')).toBe('text #hashtag# here')
    })

    test('does not touch a hashtag glued to a word', () => {
        expect(convertXHashtagsToBiliFormat('text#hashtag')).toBe('text#hashtag')
    })

    test('does not touch pure numeric tags', () => {
        expect(convertXHashtagsToBiliFormat('#1234')).toBe('#1234')
        expect(convertXHashtagsToBiliFormat('see #1234_567')).toBe('see #1234_567')
    })

    test('wraps tags containing underscores and digits when a letter is present', () => {
        expect(convertXHashtagsToBiliFormat('a #hash_tag b')).toBe('a #hash_tag# b')
        expect(convertXHashtagsToBiliFormat('a #hash_tag2 b')).toBe('a #hash_tag2# b')
    })

    test('wraps Japanese hashtags', () => {
        expect(convertXHashtagsToBiliFormat('これは #日本語ハッシュタグ です')).toBe('これは #日本語ハッシュタグ# です')
    })

    test('normalizes a full-width ＃ marker to the half-width paired form', () => {
        expect(convertXHashtagsToBiliFormat('＃日本語 です')).toBe('#日本語# です')
    })

    test('stops at ASCII hyphen like X does', () => {
        expect(convertXHashtagsToBiliFormat('#COVID-19')).toBe('#COVID#-19')
    })

    test('keeps the Japanese prolonged sound mark inside the tag', () => {
        expect(convertXHashtagsToBiliFormat('#COVIDー19')).toBe('#COVIDー19#')
    })

    test('keeps the katakana middle dot inside the tag', () => {
        expect(convertXHashtagsToBiliFormat('#ナナニジ・計算中')).toBe('#ナナニジ・計算中#')
    })

    test('handles hashtags at start and end of text', () => {
        expect(convertXHashtagsToBiliFormat('#start middle #end')).toBe('#start# middle #end#')
    })

    test('wraps multiple hashtags in one text', () => {
        expect(convertXHashtagsToBiliFormat('#ナナニジ の番組 #計算中 です')).toBe('#ナナニジ# の番組 #計算中# です')
    })

    test('is idempotent over an already converted text', () => {
        const once = convertXHashtagsToBiliFormat('text #hashtag and #COVID-19')
        expect(once).toBe('text #hashtag# and #COVID#-19')
        expect(convertXHashtagsToBiliFormat(once)).toBe(once)
    })

    test('leaves already paired Bilibili hashtags untouched', () => {
        expect(convertXHashtagsToBiliFormat('已经 #包好# 的话题')).toBe('已经 #包好# 的话题')
        expect(convertXHashtagsToBiliFormat('＃包好＃ full-width')).toBe('＃包好＃ full-width')
    })

    test('does not convert a hashtag inside an html entity-like sequence', () => {
        expect(convertXHashtagsToBiliFormat('a &#123; b')).toBe('a &#123; b')
    })

    test('never lets a newline inside the emitted pair', () => {
        expect(convertXHashtagsToBiliFormat('#tag\nnext line')).toBe('#tag#\nnext line')
    })

    test('handles hashtag at line start after a newline', () => {
        expect(convertXHashtagsToBiliFormat('first line\n#linetag end')).toBe('first line\n#linetag# end')
    })

    test('leaves texts without markers untouched', () => {
        expect(convertXHashtagsToBiliFormat('no hashtags here')).toBe('no hashtags here')
        expect(convertXHashtagsToBiliFormat('')).toBe('')
    })

    test('does not swallow trailing punctuation', () => {
        expect(convertXHashtagsToBiliFormat('看看 #ナナニジ！')).toBe('看看 #ナナニジ#！')
        expect(convertXHashtagsToBiliFormat('#tag, next')).toBe('#tag#, next')
    })
})
