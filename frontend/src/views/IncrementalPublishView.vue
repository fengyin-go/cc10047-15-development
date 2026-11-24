<template>
  <section class="page" data-module="incremental_publish">
    <header class="page-head">
      <div>
        <h2>增量发布</h2>
        <p class="page-desc">
          增量写入批次与版本可见之间的发布状态记录：确认前可撤销，同一事务只收敛一个终态，
          快照清单仅采集已确认事务的数据行。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记增量事务
        </button>
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新快照列表</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">事务总数</span>
        <strong class="stat-value">{{ todo?.total ?? rows.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待发布事务</span>
        <strong class="stat-value">{{ todo?.pending ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">终态事务</span>
        <strong class="stat-value">{{ todo?.terminal ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">并发冲突留痕</span>
        <strong class="stat-value" :data-testid="`conflict-total`">{{ todo?.conflict_count ?? 0 }}</strong>
      </article>
    </div>

    <h3 class="section-title">发布待办（按湖表汇总）</h3>
    <table class="data-table" data-testid="todo-table">
      <thead>
        <tr>
          <th>目标湖表</th>
          <th>待发布</th>
          <th>已确认</th>
          <th>已撤销</th>
          <th>快照当前可读行</th>
          <th>快照是否可读</th>
          <th>可接续起始偏移</th>
          <th>待接续事务</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in todo?.tables ?? []" :key="item.table" :data-testid="`todo-row-${item.table}`">
          <td>{{ item.table }}</td>
          <td>{{ item.pending }}</td>
          <td>{{ item.confirmed }}</td>
          <td>{{ item.aborted }}</td>
          <td data-testid="readable-rows">{{ item.readable_rows }}</td>
          <td>
            <span :class="['state-pill', item.snapshot_readable ? 'ok' : 'muted']">
              {{ item.snapshot_readable ? '可读' : '暂不可读' }}
            </span>
          </td>
          <td data-testid="resume-offset">{{ item.resumable.resume_from_offset }}</td>
          <td>
            <template v-if="item.resumable.pending_txn.length || item.resumable.aborted_txn.length">
              <span v-for="txn in [...item.resumable.pending_txn, ...item.resumable.aborted_txn]" :key="txn">
                {{ txn }}
              </span>
            </template>
            <span v-else>—</span>
          </td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">增量事务列表</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>发布状态</th>
          <th>终态操作 / 工人</th>
          <th>冲突次数</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in rows"
          :key="String(row.id)"
          :data-testid="`row-${row.id}`"
          :data-status="String(row.status)"
        >
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            <span :class="['state-pill', statusClass(String(row.status))]">{{ row.status }}</span>
          </td>
          <td>
            <template v-if="row['终态操作']">
              {{ row['终态操作'] }} / {{ row['终态工人'] }}
            </template>
            <span v-else>—</span>
          </td>
          <td :data-testid="`conflict-${row.id}`">{{ row['冲突次数'] ?? 0 }}</td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              data-testid="view-detail"
              @click="openDetail(row)"
            >
              事务详情
            </button>
            <button
              v-if="row.status === '待发布'"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-确认发布`"
              :disabled="busyId === row.id"
              @click="resolve(row, '确认发布')"
            >
              确认发布
            </button>
            <button
              v-if="row.status === '待发布'"
              class="link danger"
              type="button"
              :data-testid="`action-${row.id}-撤销发布`"
              :disabled="busyId === row.id"
              @click="resolve(row, '撤销发布')"
            >
              撤销发布
            </button>
            <button
              v-if="row.status !== '待发布'"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-确认发布`"
              @click="resolve(row, '确认发布')"
            >
              重复确认
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 4" class="empty-state">暂无增量事务，可先登记一笔</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记增量事务</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal" @click.self="detail = null">
      <div class="modal detail-modal">
        <h3>事务详情 · {{ detail.entry['事务标识'] }}</h3>

        <section class="detail-block">
          <h4>终态证据</h4>
          <dl class="evidence-grid" data-testid="terminal-evidence">
            <dt>当前状态</dt>
            <dd>
              <span :class="['state-pill', statusClass(detail.terminal_evidence.state)]">
                {{ detail.terminal_evidence.state }}
              </span>
            </dd>
            <dt>终态操作</dt>
            <dd>{{ detail.terminal_evidence.action ?? '—' }}</dd>
            <dt>完成工人</dt>
            <dd>{{ detail.terminal_evidence.worker ?? '—' }}</dd>
            <dt>终态时间</dt>
            <dd>{{ detail.terminal_evidence.time ?? '—' }}</dd>
            <dt>已发布版本</dt>
            <dd data-testid="published-version">{{ detail.terminal_evidence.published_version ?? '未发布（已撤销或待发布）' }}</dd>
            <dt>终态证据哈希</dt>
            <dd class="mono">{{ detail.terminal_evidence.evidence ?? '—' }}</dd>
            <dt>冲突次数</dt>
            <dd data-testid="detail-conflict-count">{{ detail.terminal_evidence.conflict_count }}</dd>
          </dl>
        </section>

        <section class="detail-block">
          <h4>可读行核对与可接续范围</h4>
          <p class="detail-line">
            新快照当前可读行：<strong data-testid="detail-readable-rows">{{ detail.readable_rows }}</strong>
            （仅统计已确认事务所含数据行）
          </p>
          <p class="detail-line">
            可接续起始偏移：<strong>{{ detail.resumable.resume_from_offset }}</strong>
            ，已确认连续偏移终点：{{ detail.resumable.confirmed_end_offset }}
          </p>
          <p class="detail-line">
            待发布事务：{{ detail.resumable.pending_txn.join('、') || '无' }}
            ；已撤销事务：{{ detail.resumable.aborted_txn.join('、') || '无' }}
          </p>
        </section>

        <section class="detail-block">
          <h4>快照可采集清单（已确认事务）</h4>
          <ul class="manifest-list" data-testid="manifest-list">
            <li v-for="item in detail.confirmed_manifest" :key="String(item.id)">
              {{ item['事务标识'] }} · {{ item['已发布版本'] }} ·
              偏移 {{ item['起始偏移'] }}–{{ item['结束偏移'] }} · {{ item['数据行数'] }} 行
            </li>
            <li v-if="!detail.confirmed_manifest.length" class="empty-state">暂无已确认事务，快照暂不可读</li>
          </ul>
        </section>

        <section class="detail-block">
          <h4>操作历史（含冲突与断线重放留痕）</h4>
          <ul class="history-list" data-testid="history-list">
            <li v-for="(item, index) in detail.operation_history" :key="index">
              {{ item.time }} · {{ item.worker }} · {{ item.action }} · {{ item.result }}
            </li>
          </ul>
        </section>

        <div class="page-actions">
          <button class="btn" type="button" @click="detail = null">关闭</button>
          <button
            v-if="detail.entry.status === '待发布'"
            class="btn primary"
            type="button"
            :disabled="busyId === detail.entry.id"
            @click="resolve(detail.entry, '确认发布')"
          >
            确认发布
          </button>
          <button
            v-if="detail.entry.status === '待发布'"
            class="btn danger"
            type="button"
            :disabled="busyId === detail.entry.id"
            @click="resolve(detail.entry, '撤销发布')"
          >
            撤销发布
          </button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ rows.length }} 笔增量事务</span>
      <span v-if="errorMessage" class="error-text" data-testid="error-text">{{ errorMessage }}</span>
      <span v-if="infoMessage" class="info-text" data-testid="info-text">{{ infoMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  createEntry,
  listEntries,
  loadPublishTodo,
  loadTransactionDetail,
  moduleMeta,
  resolvePublish,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'
import type { PublishTodo, TransactionDetail } from '@/api/backend-service'

const meta = moduleMeta('incremental_publish')
const rows = ref<EntryRow[]>([])
const todo = ref<PublishTodo | null>(null)
const detail = ref<TransactionDetail | null>(null)
const errorMessage = ref('')
const infoMessage = ref('')
const showCreate = ref(false)
const creating = ref(false)
const busyId = ref<number | null>(null)
const createValues = ref<Record<string, string>>({})

function statusClass(status: string): string {
  if (status === '已确认') return 'ok'
  if (status === '已撤销') return 'danger'
  return 'pending'
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function resolve(row: EntryRow, action: string) {
  errorMessage.value = ''
  infoMessage.value = ''
  busyId.value = Number(row.id)
  try {
    const worker = String(row['操作工人'] ?? '值班同学')
    const result = await resolvePublish(Number(row.id), action, worker)
    if (result.conflict) {
      infoMessage.value = `并发冲突：${result.message}`
    } else if (result.idempotent_replay) {
      infoMessage.value = `断线/重复提交幂等回放：${result.message}`
    } else {
      infoMessage.value = result.message
    }
    await reload()
    if (detail.value && Number(detail.value.entry.id) === Number(row.id)) {
      detail.value = await loadTransactionDetail(Number(row.id))
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '终态提交失败'
  } finally {
    busyId.value = null
  }
}

async function openDetail(row: EntryRow) {
  errorMessage.value = ''
  try {
    detail.value = await loadTransactionDetail(Number(row.id))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '事务详情读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const [page, todoPayload] = await Promise.all([
      listEntries(meta.key),
      loadPublishTodo(),
    ])
    rows.value = page.items
    todo.value = todoPayload
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
