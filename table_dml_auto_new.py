Option Explicit

Dim tbl, col
Dim output
output = "Inspecting VARCHAR2 columns for length semantics:" & vbCrLf & vbCrLf

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        output = output & "Table: " & tbl.Code & vbCrLf
        For Each col In tbl.Columns
            If UCase(col.DataType) = "VARCHAR2" Then
                output = output & "  Column: " & col.Code & vbCrLf
                output = output & "    Length: " & col.Length & vbCrLf
                output = output & "    LengthSemantics: '" & col.LengthSemantics & "'" & vbCrLf
            End If
        Next
        output = output & vbCrLf
    End If
Next

MsgBox output, vbOKOnly, "DEBUG: VARCHAR2 Column Semantics"
