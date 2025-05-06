export async function fetchDueTasks(hotelId: string) {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/compliance/due-tasks/${hotelId}`);
  if (!res.ok) throw new Error('Failed to fetch due tasks');
  return res.json();
}

export async function acknowledgeNextMonthTask(hotelId: string, taskId: string) {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/compliance/acknowledge-task`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ hotel_id: hotelId, task_id: taskId }),
  });
  if (!res.ok) throw new Error('Failed to acknowledge task');
  return res.json();
}
