'use client';

import { useState, useCallback, useMemo } from 'react';

interface FileState {
  file: File;
  status: 'pending' | 'uploading' | 'completed' | 'error';
  message: string;
}

export default function UtilitiesUploadBox() {
  const [dragActive, setDragActive] = useState(false);
  const [files, setFiles] = useState<Record<string, FileState>>({});
  const [billDate, setBillDate] = useState<string>(new Date().toISOString().substring(0, 10));

  const hotelId = useMemo(() => {
    if (typeof window !== 'undefined') {
      const parts = window.location.pathname.split('/');
      return parts[2] || 'unknown';
    }
    return 'unknown';
  }, []);

  const uploadFile = useCallback(async (file: File): Promise<void> => {
    const fileKey = file.name;
    
    setFiles(prev => ({
      ...prev,
      [fileKey]: {
        file,
        status: 'pending',
        message: 'Validating...'
      }
    }));

    // Basic validation
    if (!file.name.toLowerCase().endsWith('.pdf')) {
      setFiles(prev => ({
        ...prev,
        [fileKey]: { ...prev[fileKey], status: 'error', message: 'Only PDF files allowed' }
      }));
      return;
    }

    if (file.size > 10 * 1024 * 1024) {
      setFiles(prev => ({
        ...prev,
        [fileKey]: { ...prev[fileKey], status: 'error', message: 'File too large (max 10MB)' }
      }));
      return;
    }

    setFiles(prev => ({
      ...prev,
      [fileKey]: { ...prev[fileKey], status: 'uploading', message: 'Uploading...' }
    }));

    const formData = new FormData();
    formData.append('file', file);
    formData.append('hotel_id', hotelId);
    formData.append('bill_date', billDate);

    try {
      const response = await fetch('/api/utilities/parse-and-save', {
        method: 'POST',
        body: formData,
      });

      const result = await response.json();

      if (response.ok) {
        setFiles(prev => ({
          ...prev,
          [fileKey]: { ...prev[fileKey], status: 'completed', message: 'Processing in background' }
        }));
      } else {
        setFiles(prev => ({
          ...prev,
          [fileKey]: { ...prev[fileKey], status: 'error', message: result.detail || 'Upload failed' }
        }));
      }
    } catch (error) {
      setFiles(prev => ({
        ...prev,
        [fileKey]: { ...prev[fileKey], status: 'error', message: 'Network error' }
      }));
    }
  }, [hotelId, billDate]);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
    
    const droppedFiles = Array.from(e.dataTransfer.files);
    droppedFiles.forEach(uploadFile);
  }, [uploadFile]);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'pending': return '#ffa500';
      case 'uploading': return '#007bff';
      case 'completed': return '#28a745';
      case 'error': return '#dc3545';
      default: return '#6c757d';
    }
  };

  return (
    <div style={{ padding: '2rem', maxWidth: '800px', margin: '0 auto' }}>
      <h1>Utility Bill Upload</h1>
      
      <div style={{ marginBottom: '2rem' }}>
        <label>
          Bill Date:
          <input
            type="date"
            value={billDate}
            onChange={(e) => setBillDate(e.target.value)}
            style={{ marginLeft: '1rem', padding: '0.5rem' }}
          />
        </label>
      </div>

      <div
        onDrop={handleDrop}
        onDragOver={(e) => { e.preventDefault(); setDragActive(true); }}
        onDragLeave={() => setDragActive(false)}
        style={{
          border: dragActive ? '3px dashed #007bff' : '2px dashed #ccc',
          padding: '3rem',
          textAlign: 'center',
          background: dragActive ? '#f8f9fa' : 'white',
          borderRadius: '8px',
          marginBottom: '2rem'
        }}
      >
        <div style={{ fontSize: '2rem', marginBottom: '1rem' }}>
          {dragActive ? '📥' : '📄'}
        </div>
        <div>
          {dragActive ? 'Drop files here' : 'Drag & drop PDF files here'}
        </div>
      </div>

      {Object.entries(files).map(([fileName, fileState]) => (
        <div key={fileName} style={{
          padding: '1rem',
          border: '1px solid #ddd',
          borderRadius: '4px',
          marginBottom: '1rem',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center'
        }}>
          <div>
            <div style={{ fontWeight: 'bold' }}>{fileName}</div>
            <div style={{ color: getStatusColor(fileState.status), fontSize: '0.9rem' }}>
              {fileState.message}
            </div>
          </div>
          <div style={{
            padding: '0.25rem 0.5rem',
            borderRadius: '4px',
            color: 'white',
            backgroundColor: getStatusColor(fileState.status),
            fontSize: '0.8rem'
          }}>
            {fileState.status}
          </div>
        </div>
      ))}
    </div>
  );
}
