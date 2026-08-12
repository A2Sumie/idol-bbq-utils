import { expect, test } from 'bun:test'
import {
    extractShowroomEventsByRule,
    isShowroomCandidatePost,
    normalizeLlmEvents,
} from './showroom-schedule-service'

const STAFF_POST_FIXTURE = `#227出演情報

📅明日8/10(月)

🎭14:30/18:30 望月りの 出演
　歌劇版『斬舞踊』
📍東京・両国シアターXカイ
https://zanbuyou.engekijin.com

🖤🎥19:30 折本美玲・橘茉奈・南伊織
　 SHOWROOM配信
https://www.showroom-live.com/r/nanabun3rd?t=1786274207

#ナナニジ #ナナニジ3rd`

test('isShowroomCandidatePost filters staff posts', () => {
    expect(isShowroomCandidatePost(STAFF_POST_FIXTURE)).toBe(true)
    expect(isShowroomCandidatePost('#227出演情報\n📻21:00 #サリラジ\n#ナナニジ')).toBe(false)
    expect(isShowroomCandidatePost('SHOWROOM 配信します https://www.showroom-live.com/r/nanabun3rd')).toBe(true)
})

test('extractShowroomEventsByRule parses the night staff post', () => {
    const events = extractShowroomEventsByRule(STAFF_POST_FIXTURE)
    expect(events).toHaveLength(1)
    expect(events[0]?.slug).toBe('nanabun3rd')
    expect(events[0]?.time_label).toBe('19:30')
    expect(events[0]?.date_label).toBe('8/10')
    expect(events[0]?.members).toEqual(expect.arrayContaining(['折本美玲', '橘茉奈', '南伊織']))
    expect(events[0]?.starts_at).toBeGreaterThan(0)
    expect(events[0]?.confidence).toBe(0.85)
})

test('extractShowroomEventsByRule returns empty for posts without showroom URL or date', () => {
    expect(extractShowroomEventsByRule('#227出演情報 明日8/10 ラジオ')).toHaveLength(0)
    expect(extractShowroomEventsByRule('SHOWROOM配信します')).toHaveLength(0)
})

test('normalizeLlmEvents keeps llm output and falls back to rules on empty', () => {
    const llm = {
        showroom_events: [
            {
                slug: 'mochizukirino227',
                url: 'https://www.showroom-live.com/r/mochizukirino227',
                date: '8/11',
                time: '18:00',
                time_label: '18:00',
                starts_at: 1786446000,
                members: ['望月りの'],
                confidence: 0.95,
                uncertainties: [],
            },
        ],
    }
    const events = normalizeLlmEvents(llm, [])
    expect(events).toHaveLength(1)
    expect(events[0]?.slug).toBe('mochizukirino227')
    expect(events[0]?.members).toEqual(['望月りの'])

    const fallback = extractShowroomEventsByRule(STAFF_POST_FIXTURE)
    const empty = normalizeLlmEvents(null, fallback)
    expect(empty).toHaveLength(1)
    expect(empty[0]?.slug).toBe('nanabun3rd')
})

test('normalizeLlmEvents filters malformed events', () => {
    const events = normalizeLlmEvents(
        {
            showroom_events: [
                { slug: '', url: '', time: '19:00' },
                { slug: 'kawaseuta', time: '16:30', starts_at: 1786000000 },
            ],
        },
        [],
    )
    expect(events).toHaveLength(1)
    expect(events[0]?.slug).toBe('kawaseuta')
})
