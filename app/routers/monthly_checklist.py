'use client';

import { useEffect, useState } from 'react';
import styles from '@/styles/MonthlyChecklist.module.css';

interface Props {
  hotelId: string;
  userEmail: string;
  onConfirm?: () => void; // ✅ added prop
}

interface SubtaskItem {
  label: string;
  points: number;
}

interface TaskItem {
  task_id: string;
  label: string;
  frequency: string;
  category: string;
  points: number;
  info_popup: string;
  last_confirmed_date: string | null;
  is_confirmed_this_month: boolean;
  subtasks?: SubtaskItem[];
}

export default function MonthlyChecklist({ hotelId, userEmail, onConfirm }: Props) {
  const [tasks, setTasks] = useState<TaskItem[]>([]);
  const [loading, setLoading] = useState(true);

  const now = new Date();
  const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const endOfThisMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0);

  const lastMonthName = lastMonth.toLocaleString('default', { month: 'long' });
  const deadline = endOfThisMonth.toLocaleDateString('en-GB', {
    day: 'numeric',
    month: 'long',
  });

  useEffect(() => {
    fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/compliance/monthly-checklist/${hotelId}`)
      .then((res) => res.json())
      .then((data) => {
        const unconfirmed = data.filter((task: TaskItem) => !task.is_confirmed_this_month);
        setTasks(unconfirmed);
        setLoading(false);
      });
  }, [hotelId]);

  const confirmTask = async (taskId: string) => {
    await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/compliance/confirm-task`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hotel_id: hotelId, task_id: taskId, user_email: userEmail }),
    });

    setTasks((prev) => prev.filter((task) => task.task_id !== taskId));

    // ✅ trigger parent refresh
    if (onConfirm) onConfirm();
  };

  if (loading) return <p>Loading checklist...</p>;

  return (
    <div>
      <p style={{ fontSize: '0.95rem', marginBottom: '1.5rem', color: '#444' }}>
        Confirm that your <strong>{lastMonthName}</strong> tasks were completed — deadline is <strong>{deadline}</strong>.
      </p>

      {tasks.length === 0 ? (
        <p style={{ color: '#388e3c' }}>✅ All checklist tasks have been confirmed this month.</p>
      ) : (
        <ul className={styles.list}>
          {tasks.map((task) => (
            <li key={task.task_id} className={styles.taskItem}>
              <div>
                <strong>{task.label}</strong>
                <button
                  className={styles.infoBtn}
                  onClick={() => alert(task.info_popup)}
                  title="Task Info"
                >
                  ℹ️
                </button>
                <div className={styles.meta}>
                  <span>{task.frequency} • {task.category}</span>
                </div>
                {task.subtasks && (
                  <ul className={styles.subtaskList}>
                    {task.subtasks.map((sub, i) => (
                      <li key={i} className={styles.subtaskItem}>
                        • {sub.label} ({sub.points} pts)
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              <button
                className={styles.confirmBtn}
                onClick={() => confirmTask(task.task_id)}
              >
                Confirm
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
