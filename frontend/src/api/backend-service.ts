import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ------------------------------------------------------------------
// 增量发布：终态解析、发布待办、事务详情与快照可采集清单
// ------------------------------------------------------------------

export type ResolveResult = {
  ok: boolean
  conflict: boolean
  idempotent_replay: boolean
  message: string
  requested_action: string
  worker: string | null
  entry: EntryRow | null
}

export type PublishTodo = {
  total: number
  pending: number
  terminal: number
  conflict_count: number
  tables: {
    table: string
    pending: number
    confirmed: number
    aborted: number
    readable_rows: number
    snapshot_readable: boolean
    resumable: {
      table: string
      confirmed_end_offset: number
      resume_from_offset: number
      pending_txn: string[]
      aborted_txn: string[]
    }
  }[]
  items: EntryRow[]
}

export type TransactionDetail = {
  entry: EntryRow
  table: string
  terminal: boolean
  terminal_evidence: {
    state: string
    action: string | null
    worker: string | null
    time: string | null
    evidence: string | null
    published_version: string | null
    conflict_count: number
  }
  readable_rows: number
  resumable: PublishTodo['tables'][number]['resumable']
  confirmed_manifest: EntryRow[]
  operation_history: {
    action: string
    worker: string | null
    time: string
    result: string
    published_version?: string | null
  }[]
}

export async function resolvePublish(
  id: number,
  action: string,
  worker?: string,
): Promise<ResolveResult> {
  const response = await request(`/api/incremental_publish/${id}/resolve`, {
    method: 'POST',
    body: JSON.stringify({ action, worker: worker || null }),
  })
  if (!response.ok) throw new Error('增量发布终态提交失败')
  return await response.json()
}

export async function loadPublishTodo(): Promise<PublishTodo> {
  const response = await request('/api/incremental_publish/todo')
  if (!response.ok) throw new Error('发布待办读取失败')
  return await response.json()
}

export async function loadTransactionDetail(id: number): Promise<TransactionDetail> {
  const response = await request(`/api/incremental_publish/${id}/detail`)
  if (!response.ok) throw new Error('事务详情读取失败')
  return await response.json()
}

export async function loadTableManifest(table: string) {
  const response = await request(
    `/api/incremental_publish/table/${encodeURIComponent(table)}/manifest`,
  )
  if (!response.ok) throw new Error('快照发布清单读取失败')
  return await response.json()
}
